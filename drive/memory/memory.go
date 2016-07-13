// memory is an in memory storage backend for Shade.
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

func NewClient(c drive.Config) (drive.Client, error) {
	client := &MemoryDrive{
		config: c,
		chunks: make(map[string][]byte),
	}
	return client, nil
}

type MemoryDrive struct {
	config drive.Config
	files  [][]byte
	chunks map[string][]byte
	sync.RWMutex
}

// ListFiles retrieves all of the File objects known to the client.  The return
// is a list of sha256sums of the file object.  The keys may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *MemoryDrive) ListFiles() ([][]byte, error) {
	s.RLock()
	defer s.RUnlock()
	return s.files, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *MemoryDrive) PutFile(sha256sum, f []byte) error {
	s.Lock()
	s.files = append(s.files, sha256sum)
	s.chunks[string(sha256sum)] = f
	s.Unlock()
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *MemoryDrive) GetChunk(sha256sum []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	if chunk, ok := s.chunks[string(sha256sum)]; ok {
		return chunk, nil
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *MemoryDrive) PutChunk(sha256sum []byte, chunk []byte) error {
	s.Lock()
	defer s.Unlock()
	s.chunks[string(sha256sum)] = chunk
	return nil
}

func (s *MemoryDrive) GetConfig() drive.Config {
	return s.config
}

func (s *MemoryDrive) Local() bool { return true }
