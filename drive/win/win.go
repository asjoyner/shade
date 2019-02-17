// Package win is a test client.  It implements the Shade drive.Client API,
// and silently succeeds any attempted operation.  Read requests return nil for
// the value, as well as the error.  You may configure it to appear as a Local
// client, or not.
package win

import (
	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("win", NewClient)
}

// Drive is a test client implementation which silently succeeds
// every operation.  If you provide an OAuthConfig.ClientID, Local will report
// false and Persistent will report true.
type Drive struct {
	config drive.Config
}

// NewClient returns a client which will always win.
func NewClient(c drive.Config) (drive.Client, error) {
	return &Drive{config: c}, nil
}

// ListFiles returns no data, no error, every time.
func (s *Drive) ListFiles() ([][]byte, error) {
	return nil, nil
}

// GetFile returns no data, no error, every time.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	return nil, nil
}

// PutFile returns success, every time.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	return nil
}

// ReleaseFile returns success, every time.
func (s *Drive) ReleaseFile(sha256sum []byte) error {
	return nil
}

// GetChunk returns no data, no error, every time.
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
	return nil, nil
}

// PutChunk returns success, every time.
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte, f *shade.File) error {
	return nil
}

// ReleaseChunk returns success, every time.
func (s *Drive) ReleaseChunk(sha256sum []byte) error {
	return nil
}

// Warm is unnecessary for this client.
func (s *Drive) Warm(chunks [][]byte, f *shade.File) {
	return
}

// GetConfig returns an empty config.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns the value of Drive.Local.
func (s *Drive) Local() bool { return s.config.OAuth.ClientID == "" }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return s.config.OAuth.ClientID != "" }

// NewChunkLister returns an iterator which returns no chunks, no errors.
func (s *Drive) NewChunkLister() drive.ChunkLister {
	return &ChunkLister{}
}

// ChunkLister allows iterating the complete lack of chunks.
type ChunkLister struct {
}

// Next always returns false, because there are no chunks.
func (c *ChunkLister) Next() bool {
	return false
}

// Sha256 returns nil.  It should never be called, because there are no chunks.
func (c *ChunkLister) Sha256() []byte {
	return nil
}

// Err returns precisely no errors.
func (c *ChunkLister) Err() error {
	return nil
}
