package shade

import (
	"encoding/json"
	"fmt"
	"time"
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

	// Chunksize is the size of each plaintext Chunk, in bytes.
	Chunksize int

	// AesKey is a 256 bit key used to encrypt the Chunks with AES-GCM.  If no
	// key is provided, the blocks are not encrypted.  The GCM nonce is stored at
	// the front of the encrypted Chunk using gcm.Seal(); use gcm.Open() to
	// recover the Nonce when decrypting.  Nb: This increases the encrypted
	// Chunk's size by gcm.NonceSize(), currently 12 bytes.
	AesKey []byte `json:",omitempty"`
}

// Chunk represents a portion of the content of the File being stored.
type Chunk struct {
	Index  int
	Sha256 []byte
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

func (c *Chunk) String() string {
	return fmt.Sprintf("{Index: %d, Sha256: %x}", c.Index, c.Sha256)
}
