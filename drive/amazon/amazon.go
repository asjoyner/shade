package amazon

import "github.com/asjoyner/shade/drive"

func NewClient(c drive.Config) (drive.Client, error) {
	return &AmazonCloudDrive{}, nil
}

type AmazonCloudDrive struct{}

// GetFiles retrieves all of the File objects known to the client.
// The responses are marshalled JSON, which may be encrypted.
func (s *AmazonCloudDrive) GetFiles() ([]string, error) {
	return nil, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *AmazonCloudDrive) PutFile(f string) error {
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *AmazonCloudDrive) GetChunk(sha256 []byte) ([]byte, error) {
	return nil, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *AmazonCloudDrive) PutChunk(sha256 []byte, chunk []byte) ([]byte, error) {
	return nil, nil
}
