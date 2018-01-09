package shade

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"
)

var (
	chunksize = flag.Int("chunksize", 16*1024*1024, "size of a chunk, in bytes")
)

// File represents the metadata of a file stored in Shade.  It is stored and
// retrieved by the drive.Client API, and boiled down
type File struct {
	// Filename is a fully qualified path, with no leading slash.
	Filename string
	Filesize int64 // Bytes

	// ModifiedTime represents the "commit" time of this File object.  A given
	// Filename is represented by the valid File with the latest ModifiedTime.
	ModifiedTime time.Time

	// Chunks represets an ordered list of the bytes in the file.
	Chunks []Chunk

	// Chunksize is the maximum size of each plaintext Chunk, in bytes.
	Chunksize int

	// LastChunksize is the size of the last chunk in the File.  Storing this
	// explicity avoids the need to fetch the last chunk to update the Filesize.
	LastChunksize int

	// Deleted indicates all previous versions of this file should be suppressed.
	Deleted bool

	// AesKey is a 256 bit key used to encrypt the Chunks with AES-GCM.  If no
	// key is provided, the blocks are not encrypted.  The GCM nonce is stored at
	// the front of the encrypted Chunk using gcm.Seal(); use gcm.Open() to
	// recover the Nonce when decrypting.  Nb: This increases the encrypted
	// Chunk's size by gcm.NonceSize(), currently 12 bytes.
	AesKey *[32]byte
}

func NewFile(filename string) *File {
	return &File{
		Filename:     filename,
		ModifiedTime: time.Now(),
		Chunksize:    *chunksize,
		AesKey:       NewSymmetricKey(),
	}
}

// Chunk represents a portion of the content of the File being stored.
type Chunk struct {
	Index  int
	Sha256 []byte
	Nonce  []byte // If encrypted, use this Nonce to store/retrieve the Sum.
}

func (f *File) String() string {
	out := fmt.Sprintf("{Filename: %q, Filesize: %d, Chunksize: %d, AesKey: %q, Chunks:", f.Filename, f.Filesize, f.Chunksize, f.AesKey)
	sep := ", "
	if len(f.Chunks) < 2 {
		out += " "
	} else {
		out += "\n"
		sep = ",\n"
	}
	for i, c := range f.Chunks {
		if i == len(f.Chunks) {
			out += c.String() + sep
		} else {
			out += c.String()
		}
	}
	return out
}

// ToJSON returns a JSON representation of the File struct.
func (f *File) ToJSON() ([]byte, error) {
	fj, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file %x: %s", f.Filename, err)
	}
	return fj, nil
}

// FromJSON populates the fields of this File struct from a JSON representation.
// It primarily provides a convenient error message if this fails.
func (f *File) FromJSON(fj []byte) error {
	if err := json.Unmarshal(fj, f); err != nil {
		return fmt.Errorf("failed to unmarshal sha256sum %x: %s", SumString(fj), err)
	}
	return nil
}

// UpdateFilesize calculates the size of the assocaited Chunks and sets the
// Filesize member of the struct.
func (f *File) UpdateFilesize() {
	f.Filesize = int64((len(f.Chunks) - 1) * f.Chunksize)
	f.Filesize += int64(f.LastChunksize)
}

func NewChunk() Chunk {
	return Chunk{Nonce: NewNonce()}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{Index: %d, Sha256: %x}", c.Index, c.Sha256)
}

// NewSymmetricKey generates a random 256-bit AES key for File{}s.
// It panics if the source of randomness fails.
func NewSymmetricKey() *[32]byte {
	key := [32]byte{}
	_, err := io.ReadFull(rand.Reader, key[:])
	if err != nil {
		panic(err)
	}
	return &key
}

// NewNonce generates a random Nonce for AES-GCM.
// It panics if the source of randomness fails.
func NewNonce() []byte {
	nonce := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, nonce)
	if err != nil {
		panic(err)
	}
	return nonce
}
