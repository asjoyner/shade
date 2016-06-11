package amazon

import (
	"net/http"

	"github.com/asjoyner/shade/drive"
)

func NewClient(c drive.Config) (drive.Client, error) {
	client, err := getOAuthClient(c)
	if err != nil {
		return nil, err
	}
	ep, err := NewEndpoint(client) // endpoint.go
	if err != nil {
		return nil, err
	}
	return &AmazonCloudDrive{client: client, ep: ep}, nil
}

type AmazonCloudDrive struct {
	client *http.Client
	ep     *Endpoint
}

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
func (s *AmazonCloudDrive) PutChunk(sha256 []byte, chunk []byte) error {
	//s.client.Post(...bytes.NewReader(chunk)
	return nil
}
