package google

import "github.com/asjoyner/shade/drive"

func init() {
	drive.RegisterProvider("google", NewClient)
}

func NewClient(c drive.Config) (drive.Client, error) {
	return &GoogleDrive{}, nil
}

type GoogleDrive struct {
	config drive.Config
}

// ListFiles retrieves all of the File objects known to the client, and returns
// the corresponding sha256sum of the file object.  Those may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *GoogleDrive) ListFiles() ([][]byte, error) {
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

func (s *GoogleDrive) GetConfig() drive.Config {
	return s.config
}

func (s *GoogleDrive) Local() bool { return false }
