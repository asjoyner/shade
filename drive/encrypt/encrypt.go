// Package encrypt is an interface to manage encrypted storage backends.
// It presents an unencrypted interface to callers, and stores bytes in its
// Children encrypting the bytes written to it, and decrypting them again when
// requested.
//
// Symmetric encryption of each Chunk is implemented as 256-bit AES-GCM with
// random 96-bit nonces.  The AES key is encrypted with the provided RSA public
// key, the encrypted key and chunk are wrapped in a JSON message and written
// to the provided client.  This process is reversed when the bytes are read.
//
// Nb: This package does not currently encrypt the SHA sums of the files or
// chunks that are stored, only the data bytes.  See the comments in
// EncryptChunk for an explanation of why.
package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("cache", NewClient)
}

// NewClient performs sanity checking and returns a Drive client.
func NewClient(c drive.Config) (drive.Client, error) {
	d := &Drive{}
	var err error
	// Decode and verify RSA pub/priv keys
	if len(c.RsaPrivateKey) > 0 {
		key, err := x509.ParsePKCS1PrivateKey(c.RsaPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse PKCS1 encoded private key from config: %s", err)
		}
		d.privkey = key
		d.pubkey = &key.PublicKey
	} else if len(c.RsaPublicKey) > 0 {
		pubkey, err := x509.ParsePKIXPublicKey(c.RsaPublicKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DER encoded public key from config: %s", err)
		}
		rsapubkey, ok := pubkey.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("DER encoded public key in config must be an RSA key: %s", err)
		}
		d.pubkey = rsapubkey
	} else {
		return nil, fmt.Errorf("You must specify either a public or private key.")
	}

	// Initialize the child client
	if len(c.Children) == 0 {
		return nil, errors.New("no clients provided")
	}
	if len(c.Children) > 1 {
		return nil, errors.New("only one encrypted child is supported, you probably want a drive/cache in your config")
	}
	child, err := drive.NewClient(c.Children[0])
	if err != nil {
		return nil, fmt.Errorf("initing encrypted client: %s", c.Provider, err)
	}
	d.client = child
	if child.GetConfig().Write {
		d.config.Write = true
	}
	return d, nil
}

// Drive protects the contents of a single child drive.Client.  It can return a
// config which describes only its name.
//
// If any of its clients are not Local(), it reports itself as not Local() by
// returning false.  If any of its clients are Persistent(), it requires writes
// to at least one of those backends to succeed, and reports itself as
// Persistent().
type Drive struct {
	config  drive.Config
	client  drive.Client
	pubkey  *rsa.PublicKey
	privkey *rsa.PrivateKey
}

// encryptedObj is used to store a shade.File and shade.Chunk objects in the
// child client.
type encryptedObj struct {
	Key   []byte // the symmetric key, an AES 256 key
	Bytes []byte // the provided shdae.File object
}

// ListFiles retrieves all of the File objects known to the child
// client.  The return is a list of sha256sums of the file object.  The keys
// may be passed to GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	return s.client.ListFiles()
}

// PutFile encrypts and writes the metadata describing a new file.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}
	jm, err := s.EncryptChunk(f)
	if err != nil {
		return fmt.Errorf("encrypting file %x: %s", sha256sum, err)
	}
	if err := s.client.PutFile(sha256sum, jm); err != nil {
		return fmt.Errorf("writing encrypted file: %x", sha256sum)
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum.  It will be returned
// from the first client in the slice of structs that returns the chunk.
func (s *Drive) GetChunk(sha256sum []byte) ([]byte, error) {
	encryptedChunk, err := s.client.GetChunk(sha256sum)
	if err != nil {
		return nil, err
	}
	chunk, err := s.DecryptChunk(encryptedChunk)
	if err != nil {
		return nil, fmt.Errorf("decrypting file %x: %s", sha256sum, err)
	}
	return chunk, nil
}

// PutChunk writes a chunk associated with a SHA-256 sum.  It will attempt to write to
// all shade backends configured to Write.  If any backends are Persistent, it
// returns an error if all Persistent backends fail to write.
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}
	jm, err := s.EncryptChunk(chunk)
	if err != nil {
		return fmt.Errorf("encrypting file: %x", sha256sum)
	}
	if err := s.client.PutChunk(sha256sum, jm); err != nil {
		return fmt.Errorf("writing encrypted file: %x", sha256sum)
	}
	return nil
}

// EncryptChunk accepts a sha256sum and bytes, and returns a suitably encrypted
// copy of those bytes for persistent storage.  It uses the following process:
//  - generates a new 256-bit encryption key
//  - uses the new key to encrypt the provided File's bytes
//  - RSA encrypts that key (but not the bytes' sha256sum)
//  - bundles the encrypted key and encrypted File bytes as an EncryptedFile
//  - marshals the EncryptedFile as JSON and returns it
func (s *Drive) EncryptChunk(f []byte) ([]byte, error) {
	rng := rand.Reader
	key := NewEncryptionKey()
	encryptedKey, err := rsa.EncryptOAEP(sha256.New(), rng, s.pubkey, key[:], nil)
	if err != nil {
		return nil, fmt.Errorf("could not encrypt key: %s", err)
	}
	// It would be nice to properly encrypt the sha256sum, but doing it right
	// requires that it isn't deterministic, which breaks the ability to look up
	// by the unencrypted-sha256sum.  When receiving a GetChunk request, this
	// module needs a repeataable method to find the sha256sum.  If you're
	// reading this, and can come up with a good way to do that (which doesn't
	// require storing and plumbing the nonce back, or make it trivial to connect
	// "input chunk Foo" to "stored at value Bar" given this code), I'm all ears.
	/*
		encryptedSum, err := rsa.EncryptOAEP(sha256.New(), rng, s.pubkey, sha256sum, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("could not encrypt sum %q: %s\n", sha256sum, err)
		}
	*/
	encryptedBytes, err := Encrypt(f, key)
	if err != nil {
		return nil, fmt.Errorf("could not encrypt contents: %s", err)
	}
	jm, err := json.Marshal(encryptedObj{Key: encryptedKey, Bytes: encryptedBytes})
	if err != nil {
		return nil, fmt.Errorf("could not marshal json: %s", err)
	}
	return jm, nil
}

// DecryptChunk reverses the behavior of EncryptChunk.
func (s *Drive) DecryptChunk(f []byte) ([]byte, error) {
	eo := &encryptedObj{}
	if err := json.Unmarshal(f, eo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal: %v", err)
	}
	rng := rand.Reader

	keySlice, err := rsa.DecryptOAEP(sha256.New(), rng, s.privkey, eo.Key, nil)
	if err != nil {
		return nil, fmt.Errorf("could not decrypt key: %s", err)
	}
	key := &[32]byte{}
	copy(key[:], keySlice)
	plaintext, err := Decrypt(eo.Bytes, key)
	if err != nil {
		return nil, fmt.Errorf("could not decrypt contents %s", err)
	}
	return plaintext, nil
}

// NewEncryptionKey generates a random 256-bit key for Encrypt() and
// Decrypt(). It panics if the source of randomness fails.
func NewEncryptionKey() *[32]byte {
	key := [32]byte{}
	_, err := io.ReadFull(rand.Reader, key[:])
	if err != nil {
		panic(err)
	}
	return &key
}

// Encrypt encrypts data using 256-bit AES-GCM.  This both hides the content of
// the data and provides a check that it hasn't been altered. Output takes the
// form nonce|ciphertext|tag where '|' indicates concatenation.
func Encrypt(plaintext []byte, key *[32]byte) (ciphertext []byte, err error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts data using 256-bit AES-GCM.  This both hides the content of
// the data and provides a check that it hasn't been altered. Expects input
// form nonce|ciphertext|tag where '|' indicates concatenation.
func Decrypt(ciphertext []byte, key *[32]byte) (plaintext []byte, err error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("malformed ciphertext")
	}

	return gcm.Open(nil,
		ciphertext[:gcm.NonceSize()],
		ciphertext[gcm.NonceSize():],
		nil,
	)
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return drive.Config{Provider: "encrypt"}
}

// Local returns true only if the configured backends is local to this machine.
func (s *Drive) Local() bool {
	return s.client.Local()
}

// Persistent returns true if the configured storage backend is Persistent().
func (s *Drive) Persistent() bool {
	return s.client.Persistent()
}
