// Package fail is a test client.  It implements the Shade drive.Client API,
// and fails any attempted operation.  You may configure it to appear as a
// Local client, or not.
package fail

import (
	"errors"

	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("fail", NewClient)
}

// Drive is a test client implementation which fails every operation.  If you
// provide any OAuthConfig its will return false for Local.
type Drive struct {
	config drive.Config
}

// NewClient returns a client which will always fail.
func NewClient(c drive.Config) (drive.Client, error) {
	return &Drive{config: c}, nil
}

// ListFiles returns an error, every time.
func (s *Drive) ListFiles() ([][]byte, error) {
	return nil, errors.New("fail.Drive does what it says on the tin")
}

// PutFile returns an error, every time.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	return errors.New("fail.Drive does what it says on the tin")
}

// GetChunk returns an error, every time.
func (s *Drive) GetChunk(sha256sum []byte) ([]byte, error) {
	return nil, errors.New("fail.Drive does what it says on the tin")
}

// PutChunk returns an error, every time.
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte) error {
	return errors.New("fail.Drive does what it says on the tin")
}

// GetConfig returns an empty config.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns true, unless any OAuthConfig is provided.
func (s *Drive) Local() bool { return s.config.OAuth.ClientID == "" }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return s.config.OAuth.ClientID != "" }
