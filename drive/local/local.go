// Package local is a local storage backend for Shade.
//
// It stores files and chunks locally to disk.  You may define full filepaths
// to store the files and chunks in the config, or via flag.  If you define
// neither, the flags will choose sensible defaults for your operating system.
package local

import (
	"encoding/hex"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

var (
	chunkCacheDir = flag.String(
		"local.chunkCacheDir",
		path.Join(shade.ConfigDir(), "local"),
		"Default path for local Drive chunk storage.",
	)
	fileCacheDir = flag.String(
		"local.fileCacheDir",
		path.Join(shade.ConfigDir(), "local"),
		"Default path for local Drive file storage.",
	)
)

func init() {
	drive.RegisterProvider("local", NewClient)
}

// NewClient returns a fully initlized local client.
func NewClient(c drive.Config) (drive.Client, error) {
	if c.ChunkParentID == "" {
		c.ChunkParentID = *chunkCacheDir
	}
	if c.FileParentID == "" {
		c.FileParentID = *fileCacheDir
	}
	for _, dir := range []string{
		c.ChunkParentID,
		c.FileParentID,
	} {
		if fh, err := os.Open(dir); err != nil {
			if err := os.Mkdir(dir, 0700); err != nil {
				return nil, err
			}
		} else {
			fh.Close()
		}
	}

	return &Drive{config: c}, nil
}

type Drive struct {
	sync.RWMutex
	config drive.Config
}

// ListFiles retrieves all of the File objects known to the client.  The return
// values are the sha256sum of the file object.  The keys may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	var resp [][]byte
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
			resp = append(resp, h)
		}
	}
	return resp, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, data []byte) error {
	s.Lock()
	defer s.Unlock()
	filename := path.Join(s.config.FileParentID, hex.EncodeToString(sha256sum))
	if fh, err := os.Open(filename); err == nil {
		fh.Close()
		return nil
	}
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256sum []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	paths := []string{s.config.FileParentID, s.config.ChunkParentID}
	for _, p := range paths {
		filename := path.Join(p, hex.EncodeToString(sha256sum))
		if f, err := ioutil.ReadFile(filename); err == nil {
			return f, nil
		}
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk to local disk
func (s *Drive) PutChunk(sha256sum []byte, data []byte) error {
	s.Lock()
	defer s.Unlock()
	filename := path.Join(s.config.ChunkParentID, hex.EncodeToString(sha256sum))
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	return nil
}

func (s *Drive) GetConfig() drive.Config {
	return s.config
}

func (s *Drive) Local() bool { return true }
