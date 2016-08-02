// Package local is a persistent local storage backend for Shade.
//
// It stores files and chunks locally to disk.  You may define full filepaths
// to store the files and chunks in the config, or via flag.  If you define
// neither, the flags will choose sensible defaults for your operating system.
package local

import (
	"encoding/hex"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/asjoyner/shade/drive"
	"github.com/google/btree"
)

func init() {
	drive.RegisterProvider("local", NewClient)
}

// NewClient returns a fully initlized local client.
func NewClient(c drive.Config) (drive.Client, error) {
	// Sanity check the config
	if c.ChunkParentID == "" {
		return nil, errors.New("specify the path to store local chunks as ChunkParentID")
	}
	if c.FileParentID == "" {
		return nil, errors.New("specify the path to store local files as FileParentID")
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

	// Initialize the internal accounting
	s := &Drive{
		config: c,
		files:  btree.New(2),
		chunks: make(map[string]int64),
	}

	// Make note of all the filenames in FileParentID
	files, err := ioutil.ReadDir(c.FileParentID)
	if err != nil {
		return nil, err
	}
	for _, fi := range files {
		if !fi.IsDir() {
			sha256sum, err := hex.DecodeString(fi.Name())
			if err != nil {
				log.Printf("file with non-hex string value name: %s", fi.Name())
				continue
			}
			s.files.ReplaceOrInsert(Chunk{
				sum:   sha256sum,
				mtime: fi.ModTime().Unix(),
			})
		}
	}

	// Count the bytes in the local storage
	chunks, err := ioutil.ReadDir(c.ChunkParentID)
	if err != nil {
		return nil, err
	}
	for _, fi := range chunks {
		if !fi.IsDir() {
			sha256sum, err := hex.DecodeString(fi.Name())
			if err != nil {
				log.Printf("file with non-hex string value name: %s", fi.Name())
				continue
			}
			s.chunks[string(sha256sum)] = fi.ModTime().Unix()
			s.chunkBytes += fi.Size()
		}
	}

	return s, nil
}

// Drive implements the drive.Client interface by storing Files and Chunks
// to the local filesystem.  It treats the ChunkParentID and FileParentID as
// filepaths to the directory to store data in.
type Drive struct {
	sync.RWMutex // serializes accesses to the directories on local disk
	config       drive.Config
	files        *btree.BTree     // for accounting
	chunks       map[string]int64 // for accounting
	chunkBytes   int64            // for accounting
}

// Chunk describes an object cached to the filesystem, in a way that the btree
// implementaiton can sort it.  This allows garbage collection by mtime.
type Chunk struct {
	sum   []byte
	mtime int64
}

// Less ultimately describes the order chunks are deleted in.
func (a Chunk) Less(bt btree.Item) bool {
	b := bt.(Chunk)
	if a.mtime < b.mtime {
		return true
	} else if a.mtime == b.mtime && string(a.sum) < string(b.sum) {
		return true
	}
	return false
}

// ListFiles retrieves all of the File objects known to the client.  The return
// values are the sha256sum of the file object.  The keys may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	var resp [][]byte
	s.Lock()
	defer s.Unlock()
	s.files.Ascend(func(item btree.Item) bool {
		resp = append(resp, item.(Chunk).sum)
		return true
	})
	return resp, nil
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, data []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.config.MaxFiles > 0 {
		if err := cleanup(s.files, s.config.FileParentID, s.config.MaxFiles-1); err != nil {
			return err
		}
	}

	filename := path.Join(s.config.FileParentID, hex.EncodeToString(sha256sum))
	if fh, err := os.Open(filename); err == nil {
		fh.Close()
		return nil
	}
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	s.files.ReplaceOrInsert(Chunk{
		sum:   sha256sum,
		mtime: time.Now().Unix(),
	})
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
	if fh, err := os.Open(filename); err == nil {
		fh.Close()
		return nil
	}
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		return err
	}
	s.chunks[string(sha256sum)] = time.Now().Unix()
	s.chunkBytes += int64(len(data))
	return nil
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether the storage is local to this machine.
func (s *Drive) Local() bool { return true }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return true }

// cleanup iterates the provided BTree and removes the oldest entries from the
// filesystem, in the provided directory, to bring the length below the
// provided maximum size.
func cleanup(files *btree.BTree, dir string, size uint64) error {
	for {
		len := files.Len()
		if len == 0 || uint64(len) > size {
			return nil
		}
		oldest := hex.EncodeToString(files.Min().(Chunk).sum)
		r := path.Join(dir, oldest)
		if err := os.Remove(r); err != nil {
			return err
		}
		files.DeleteMin()
	}
}
