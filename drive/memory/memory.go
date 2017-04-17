// Package memory is an in memory storage backend for Shade.
//
// It stores files and chunks transiently in RAM.
package memory

import (
	"errors"
	"sync"

	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("memory", NewClient)
}

// NewClient returns a Drive client, based on the provided config.
func NewClient(c drive.Config) (drive.Client, error) {
	client := &Drive{
		config: c,
		chunks: make(map[string][]byte),
	}
	return client, nil
}

// Drive implements the drive.Client interface by storing Files and Chunks
// transiently in RAM.  The provided config can be returned, but is otherwise
// ignored.
type Drive struct {
	config drive.Config
	files  [][]byte
	fm     sync.RWMutex // protects access to files
	chunks map[string][]byte
	cm     sync.RWMutex // protects access to chunks
}

// ListFiles retrieves all of the File objects known to the client.  The return
// is a list of sha256sums of the file object.  The keys may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	s.fm.RLock()
	defer s.fm.RUnlock()
	return s.files, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	s.fm.Lock()
	defer s.fm.Unlock()
	s.cm.Lock()
	defer s.cm.Unlock()
	s.files = append(s.files, sha256sum)
	s.chunks[string(sha256sum)] = f
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256sum []byte) ([]byte, error) {
	s.cm.RLock()
	defer s.cm.RUnlock()
	if chunk, ok := s.chunks[string(sha256sum)]; ok {
		// make a copy, to ensure the caller can't modify the underlying array
		retChunk := make([]byte, len(chunk))
		copy(retChunk, chunk)
		return chunk, nil
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte) error {
	s.cm.Lock()
	defer s.cm.Unlock()
	s.chunks[string(sha256sum)] = chunk
	return nil
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether the storage is local to this machine.
func (s *Drive) Local() bool { return true }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return false }

// ListChunks returns all the chunks known to the memory client.  It is helpful
// for tests.
func (s *Drive) ListChunks() [][]byte {
	s.cm.Lock()
	defer s.cm.Unlock()
	resp := make([][]byte, 0, len(s.chunks))
	for stringSum := range s.chunks {
		resp = append(resp, []byte(stringSum))
	}
	return resp
}
