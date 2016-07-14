package google

import "github.com/asjoyner/shade/drive"

func init() {
	drive.RegisterProvider("google", NewClient)
}

// NewClient returns a new Drive client.
// TODO(shanel): Should this just be New? or NewDrive?
func NewClient(c drive.Config) (drive.Client, error) {
	return &Drive{}, nil
}

// Drive represents access to the Google Drive storage system.
type Drive struct {
	config drive.Config
}

// ListFiles retrieves all of the File objects known to the client, and returns
// the corresponding sha256sum of the file object.  Those may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	return nil, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256, f []byte) error {
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256 []byte) ([]byte, error) {
	return nil, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256 []byte, chunk []byte) error {
	return nil
}

// GetConfig returns the associated drive.Config object.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether access is local.
func (s *Drive) Local() bool { return false }
