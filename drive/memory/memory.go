// memory is an in memory storage backend for Shade.
//
// It stores files and chunks transiently in RAM.
package memory

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/asjoyner/shade/drive"
)

func NewClient(c drive.Config) (drive.Client, error) {
	client := &MemoryDrive{
		config: c,
		chunks: make(map[string][]byte),
	}
	return client, nil
}

type MemoryDrive struct {
	config drive.Config
	files  []FileWithSum
	chunks map[string][]byte
	sync.RWMutex
}

type FileWithSum struct {
	file      []byte
	sha256sum []byte
}

// ListFiles retrieves all of the File objects known to the client.  The return
// maps from arbitrary unique keys to the sha256sum of the file object.  The
// keys may be passed to GetFile() to retrieve the corresponding shade.File.
func (s *MemoryDrive) ListFiles() (map[string][]byte, error) {
	resp := make(map[string][]byte)
	s.RLock()
	defer s.RUnlock()
	for i, fws := range s.files {
		resp[fmt.Sprintf("%d", i)] = fws.sha256sum
	}
	return resp, nil
}

// GetFile retrieves the File described by the ID.
// The responses are marshalled JSON, which may be encrypted.
func (s *MemoryDrive) GetFile(fileID string) ([]byte, error) {
	i, err := strconv.Atoi(fileID)
	if err != nil {
		return nil, fmt.Errorf("invalid fileID: %s", err)
	}
	s.RLock()
	defer s.RUnlock()
	if i < len(s.files) {
		return s.files[i].file, nil
	}
	return nil, errors.New("file not found")
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *MemoryDrive) PutFile(sha256sum, f []byte) error {
	s.Lock()
	s.files = append(s.files, FileWithSum{sha256sum: sha256sum, file: f})
	s.Unlock()
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *MemoryDrive) GetChunk(sha256 []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	if chunk, ok := s.chunks[string(sha256)]; ok {
		return chunk, nil
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *MemoryDrive) PutChunk(sha256 []byte, chunk []byte) error {
	s.Lock()
	defer s.Unlock()
	s.chunks[string(sha256)] = chunk
	return nil
}

func (s *MemoryDrive) GetConfig() drive.Config {
	return s.config
}
