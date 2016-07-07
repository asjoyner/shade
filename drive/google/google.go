package google

import "github.com/asjoyner/shade/drive"

func NewClient(c drive.Config) (drive.Client, error) {
	return &GoogleDrive{}, nil
}

type GoogleDrive struct{}

// ListFiles retrieves all of the File objects known to the client.  The return
// maps from arbitrary unique keys to the sha256sum of the file object.  The
// keys may be passed to GetFile() to retrieve the corresponding shade.File.
func (s *GoogleDrive) ListFiles() (map[string][]byte, error) {
	return nil, nil
}

// GetFiles retrieves all of the File objects known to the client.
// The responses are marshalled JSON, which may be encrypted.
func (s *GoogleDrive) GetFile(fileID string) ([]byte, error) {
	return nil, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *GoogleDrive) PutFile(sha256, f []byte) error {
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *GoogleDrive) GetChunk(sha256 []byte) ([]byte, error) {
	return nil, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *GoogleDrive) PutChunk(sha256 []byte, chunk []byte) error {
	return nil
}
