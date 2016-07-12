// localdrive is a local storage backend for Shade.
//
// It stores files and chunks locally to disk.  You may define full filepaths
// to store the files and chunks in the config, or via flag.  If you define
// neither, the flags will choose sensible defaults for your operating system.
package localdrive

import (
	"encoding/hex"
	"flag"
	"io/ioutil"
	"log"
	"path"
	"sync"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

var (
	chunkCacheDir = flag.String(
		"localDrive.chunkCacheDir",
		path.Join(shade.ConfigDir(), "localdrive"),
		"Default path for LocalDrive chunk storage.",
	)
	fileCacheDir = flag.String(
		"localDrive.fileCacheDir",
		path.Join(shade.ConfigDir(), "localdrive"),
		"Default path for LocalDrive file storage.",
	)
)

func NewClient(c drive.Config) (drive.Client, error) {
	if c.ChunkParentID == "" {
		c.ChunkParentID = *chunkCacheDir
	}
	if c.FileParentID == "" {
		c.FileParentID = *fileCacheDir
	}
	return &LocalDrive{config: c}, nil
}

type LocalDrive struct {
	config drive.Config
	sync.RWMutex
}

// ListFiles retrieves all of the File objects known to the client.  The return
// maps from arbitrary unique keys to the sha256sum of the file object.  The
// keys may be passed to GetFile() to retrieve the corresponding shade.File.
func (s *LocalDrive) ListFiles() (map[string][]byte, error) {
	resp := make(map[string][]byte)
	s.Lock()
	defer s.Unlock()
	nodes, err := ioutil.ReadDir(s.config.FileParentID)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if !n.IsDir() {
			h, err := hex.DecodeString(n.Name())
			if err != nil {
				log.Printf("file with non-hex string value name: %s", n.Name())
				continue
			}
			resp[string(h)] = h
		}
	}
	return resp, nil
}

// GetFile retrieves the File described by the ID.
// The responses are marshalled JSON, which may be encrypted.
func (s *LocalDrive) GetFile(fileID string) ([]byte, error) {
	s.Lock()
	defer s.Unlock()
	filename := path.Join(s.config.FileParentID, fileID)
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *LocalDrive) PutFile(sha256sum, data []byte) error {
	s.Lock()
	defer s.Unlock()
	filename := path.Join(s.config.FileParentID, hex.EncodeToString(sha256sum))
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *LocalDrive) GetChunk(sha256sum []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	filename := path.Join(s.config.ChunkParentID, hex.EncodeToString(sha256sum))
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// PutChunk writes a chunk to local disk
func (s *LocalDrive) PutChunk(sha256sum []byte, data []byte) error {
	s.Lock()
	defer s.Unlock()
	filename := path.Join(s.config.ChunkParentID, hex.EncodeToString(sha256sum))
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	return nil
}

func (s *LocalDrive) GetConfig() drive.Config {
	return s.config
}

func (s *LocalDrive) Local() bool { return true }
