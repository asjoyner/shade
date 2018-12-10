// Package encrypt is an interface to manage encrypted storage backends.
// It presents an unencrypted interface to callers by storing bytes in the
// provided Child client, encrypting the bytes written to it, and decrypting
// them again when requested.
//
// File objects are encrypted with an RSA public key provided in the config.
// If an RSA private key is provided, GetFile and ListFiles will perform the
// reverse operation.
//
// Chunk objects are encrypted with 256-bit AES-GCM using an AES key stored in
// the shade.File struct and a random 96-bit nonce stored with each shade.Chunk
// struct.
//
// The sha256sum of each Chunk is AES encrypted with the same key as the
// contents and a nonce which is stored in the corresponding shade.File struct.
// Unlike the Chunk, the nonce cannot be stored appended to the sha256sum, because
// it must be known in advance to retrieve the chunk.
// Nb: It is important not to reuse a nonce with the same key, thus callers must
// reset the Nonce in a shade.Chunk when updating the Sha256sum value.
//
// The sha256sum of File objects are not encrypted.  The struct contains
// sufficient internal randomness (Nonces of shade.Chunk objects, mtime, etc)
// that the sum does not leak information about the contents of the file.
package encrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("encrypt", NewClient)
}

// NewClient performs sanity checking and returns a Drive client.
func NewClient(c drive.Config) (drive.Client, error) {
	d := &Drive{config: c}
	var err error
	// Decode and verify RSA pub/priv keys
	if len(c.RsaPrivateKey) > 0 {
		b, _ := pem.Decode([]byte(c.RsaPrivateKey))
		if b == nil {
			return nil, fmt.Errorf("parsing PEM encoded private key from config: %s", c.RsaPrivateKey)
		}
		key, err := x509.ParsePKCS1PrivateKey(b.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PKCS1 private key from config: %s", err)
		}
		d.privkey = key
		d.pubkey = &key.PublicKey
	} else if len(c.RsaPublicKey) > 0 {
		b, _ := pem.Decode([]byte(c.RsaPublicKey))
		if b == nil {
			return nil, fmt.Errorf("parsing PEM encoded public key from config: %s", c.RsaPublicKey)
		}
		pubkey, err := x509.ParsePKIXPublicKey(b.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DER encoded public key from config: %s", err)
		}
		rsapubkey, ok := pubkey.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("DER encoded public key in config must be an RSA key: %s", err)
		}
		d.pubkey = rsapubkey
	} else {
		return nil, fmt.Errorf("encrypt requires that you specify either a public or private key")
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
		return nil, fmt.Errorf("initing encrypted client %q: %s", c.Provider, err)
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

// encryptedObj is used to store shade.File objects in the child client.
type encryptedObj struct {
	Key   []byte // the symmetric key, an AES 256 key
	Bytes []byte // the provided shdae.File object
}

// ListFiles retrieves all of the File objects known to the child
// client.  The return is a list of sha256sums of the file object.  The keys
// may be passed to GetFile() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	return s.client.ListFiles()
}

// PutFile encrypts and writes the metadata describing a new file.
// It uses the following process:
//  - generates a new 256-bit AES encryption key
//  - uses the new key to Encrypt() the provided File's bytes
//  - RSA encrypts the AES key (but not the sha256sum of the File's bytes)
//  - bundles the encrypted key and encrypted bytes as an encryptedObj
//  - marshals the encryptedObj as JSON and store it in the child client, at
//    the value of the sha256sum of the plaintext
func (s *Drive) PutFile(sha256sum, f []byte) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}
	key := shade.NewSymmetricKey()
	rng := rand.Reader
	encryptedKey, err := rsa.EncryptOAEP(sha256.New(), rng, s.pubkey, key[:], nil)
	if err != nil {
		return fmt.Errorf("could not encrypt key: %s", err)
	}
	encryptedBytes, err := Encrypt(f, key)
	if err != nil {
		return fmt.Errorf("encrypting file %x: %s", sha256sum, err)
	}
	// TODO: consider making this more efficient by avoiding JSON and using a
	// fixed-size prefix to store the encryptedKey, ala gcm.Seal and gcm.Open.
	jm, err := json.Marshal(encryptedObj{Key: encryptedKey, Bytes: encryptedBytes})
	if err != nil {
		return fmt.Errorf("could not marshal json: %s", err)
	}
	if err := s.client.PutFile(sha256sum, jm); err != nil {
		return fmt.Errorf("writing encrypted file: %x", sha256sum)
	}
	return nil
}

// GetFile retrieves the file object described by the sha256sum, decrypts it,
// and returns it to the caller.  It reverses the process described in
// PutFile.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	jm, err := s.client.GetFile(sha256sum)
	if err != nil {
		return nil, fmt.Errorf("reading encrypted file %x: %s", sha256sum, err)
	}
	eo := &encryptedObj{}
	// TODO: consider making this more efficient by avoiding JSON and using a
	// fixed-size prefix to store the encryptedKey, ala gcm.Seal and gcm.Open.
	if err := json.Unmarshal(jm, eo); err != nil {
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
	if err != nil {
		return nil, fmt.Errorf("decrypting encrypted file %x: %s", sha256sum, err)
	}
	return plaintext, nil
}

// PutChunk writes a chunk associated with a SHA-256 sum.  It uses the following process:
//  - From the provided shade.File struct, retrieve:
//    - the AES key of the File
//    - the Nonce of the associated shade.Chunk struct
//  - encrypt the sha256sum with the provided Key and Nonce
//  - encrypt the bytes with the provided Key and a unique Nonce
//  - store the encrypted bytes at the encrypted sum in the child client
func (s *Drive) PutChunk(sha256sum []byte, chunkBytes []byte, f *shade.File) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}
	if f == nil {
		return errors.New("no file provided")
	}
	if f.AesKey == nil {
		return errors.New("no AES encryption key for file")
	}
	encBytes, err := Encrypt(chunkBytes, f.AesKey)
	if err != nil {
		return fmt.Errorf("encrypting file: %x", sha256sum)
	}
	encryptedSum, err := GetEncryptedSum(sha256sum, f)
	if err != nil {
		return fmt.Errorf("encrypting sha256sum %x: %s", sha256sum, err)
	}

	if err := s.client.PutChunk(encryptedSum, encBytes, f); err != nil {
		return fmt.Errorf("writing encrypted file %x: %s", sha256sum, err)
	}
	return nil
}

// GetChunk retrieves and decrypts the chunk with a given SHA-256 sum.
// It reverses the process of PutChunk, in particular, leveraging the stored
// Nonce to be able to find the encrypted sha256sum in the child client.
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
	encryptedSum, err := GetEncryptedSum(sha256sum, f)
	if err != nil {
		return nil, fmt.Errorf("encrypting sha256sum %x: %s", sha256sum, err)
	}

	encBytes, err := s.client.GetChunk(encryptedSum, f)
	if err != nil {
		return nil, err
	}
	chunkBytes, err := Decrypt(encBytes, f.AesKey)
	if err != nil {
		return nil, fmt.Errorf("decrypting file %x: %s", sha256sum, err)
	}
	return chunkBytes, nil
}

// GetEncryptedSum calculates the encrypted sha256sum that a chunk will be
// stored at, for a given chunk in a given file.  It is used both by PutChunk
// to store the chunk, and later by GetChunk to find it again.
func GetEncryptedSum(sha256sum []byte, f *shade.File) (encryptedSum []byte, err error) {
	var nonce []byte
	// Find the chunk's Nonce
	for _, chunk := range f.Chunks {
		if bytes.Equal(chunk.Sha256, sha256sum) {
			if chunk.Nonce == nil {
				return nil, fmt.Errorf("no Nonce in Chunk: %x", sha256sum)
			}
			nonce = chunk.Nonce
		}
	}
	if nonce == nil {
		return nil, fmt.Errorf("no corresponding Chunk in File: %x", sha256sum)
	}
	return encryptUnsafe(sha256sum, f.AesKey, nonce)
}

// Encrypt encrypts data using 256-bit AES-GCM.  This both hides the content of
// the data and provides a check that it hasn't been altered. Output takes the
// form nonce|ciphertext|tag where '|' indicates concatenation.
func Encrypt(plaintext []byte, key *[32]byte) (ciphertext []byte, err error) {
	return encryptUnsafe(plaintext, key, shade.NewNonce())
}

// encryptUnsafe is the internal implementation of Encrypt().  It  allows you
// to specify the key AND the nonce.  Use with caution: you must not encrypt
// two different messages with the same key and nonce!
func encryptUnsafe(plaintext []byte, key *[32]byte, nonce []byte) (ciphertext []byte, err error) {
	if key == nil {
		return nil, fmt.Errorf("no key provided")
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("Invalid nonce size, want: %d got %d", gcm.NonceSize(), len(nonce))
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
	return s.config
}

// Local returns true only if the configured backend is local to this machine.
func (s *Drive) Local() bool {
	return s.client.Local()
}

// Persistent returns true if the configured storage backend is Persistent().
func (s *Drive) Persistent() bool {
	return s.client.Persistent()
}
