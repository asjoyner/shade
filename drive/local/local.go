// Package local is a persistent local storage backend for Shade.
//
// It stores files and chunks locally to disk.  You may define full filepaths
// to store the files and chunks in the config, or via flag.  If you define
// neither, the flags will choose sensible defaults for your operating system.
package local

import (
	"encoding/hex"
	"errors"
	"expvar"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"github.com/golang/glog"
	"github.com/google/btree"
)

var (
	localFiles      = expvar.NewInt("localFiles")
	localChunks     = expvar.NewInt("localChunks")
	localChunkBytes = expvar.NewInt("localChunkBytes")
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
		chunks: btree.New(2),
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
	localFiles.Set(int64(s.files.Len()))

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
			s.chunks.ReplaceOrInsert(Chunk{
				sum:   sha256sum,
				mtime: fi.ModTime().Unix(),
			})
			s.chunkBytes += uint64(fi.Size())
		}
	}
	localChunks.Set(int64(s.chunks.Len()))
	localChunkBytes.Set(int64(s.chunkBytes))

	return s, nil
}

// Drive implements the drive.Client interface by storing Files and Chunks
// to the local filesystem.  It treats the ChunkParentID and FileParentID as
// filepaths to the directory to store data in.
type Drive struct {
	sync.RWMutex // serializes accesses to the directories on local disk
	config       drive.Config
	files        *btree.BTree // for accounting
	chunks       *btree.BTree // for accounting
	chunkBytes   uint64       // for accounting
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

// GetFile retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	return s.GetChunk(sha256sum, nil)
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
//
// TODO(asjoyner): collapse the logic in PutFile and PutChunk into shared code.
func (s *Drive) PutFile(sha256sum, data []byte) error {
	s.Lock()
	defer s.Unlock()

	filename := path.Join(s.config.FileParentID, hex.EncodeToString(sha256sum))

	// Optimize duplicate push
	if fi, err := os.Stat(filename); err == nil {
		now := time.Now()
		if err := os.Chtimes(filename, now, now); err != nil {
			glog.Warningf("updating file mtime: %s", err)
			return fmt.Errorf("could not update mtime: %s", err)
		}
		s.files.Delete(Chunk{sum: sha256sum, mtime: fi.ModTime().Unix()})
		s.files.ReplaceOrInsert(Chunk{sum: sha256sum, mtime: now.Unix()})
		return nil
	}

	if s.config.MaxFiles > 0 {
		if err := s.cleanup(true, 1); err != nil {
			glog.Warningf("file cleanup(): %s", err)
			return err
		}
	}

	if fh, err := os.Open(filename); err == nil {
		fh.Close()
		return nil
	}
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		glog.Warningf("writing file to cache: %s", err)
		return err
	}

	fi, err := os.Stat(filename)
	if err != nil {
		glog.Warningf("post-write file stat: %s", err)
		return fmt.Errorf("could not stat file after write: %s", err)
	}
	s.files.ReplaceOrInsert(Chunk{
		sum:   sha256sum,
		mtime: fi.ModTime().Unix(),
	})
	localFiles.Set(int64(s.files.Len()))
	return nil
}

// ReleaseFile deletes a file with a given SHA-256 sum
func (s *Drive) ReleaseFile(sha256sum []byte) error {
	s.Lock()
	defer s.Unlock()

	filename := path.Join(s.config.FileParentID, hex.EncodeToString(sha256sum))

	fi, err := os.Stat(filename)
	if err != nil {
		return nil // no such file: our work here is done
	}
	s.files.Delete(Chunk{sum: sha256sum, mtime: fi.ModTime().Unix()})
	if err := os.Remove(filename); err != nil {
		glog.Warningf("removed cache entry but not file: %s", err)
		return err
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
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
func (s *Drive) PutChunk(sha256sum []byte, data []byte, f *shade.File) error {
	s.Lock()
	defer s.Unlock()

	filename := path.Join(s.config.ChunkParentID, hex.EncodeToString(sha256sum))

	// Optimize duplicate push
	if fi, err := os.Stat(filename); err == nil {
		now := time.Now()
		if err := os.Chtimes(filename, now, now); err != nil {
			glog.Warningf("updating chunk mtime: %s", err)
			return fmt.Errorf("could not update mtime: %s", err)
		}
		s.chunks.Delete(Chunk{sum: sha256sum, mtime: fi.ModTime().Unix()})
		s.chunks.ReplaceOrInsert(Chunk{sum: sha256sum, mtime: now.Unix()})
		return nil
	}

	if s.config.MaxChunkBytes > 0 {
		if err := s.cleanup(false, uint64(len(data))); err != nil {
			glog.Warningf("chunk cleanup(): %s", err)
			return err
		}
	}

	if fh, err := os.Open(filename); err == nil {
		fh.Close()
		return nil
	}
	if err := ioutil.WriteFile(filename, data, 0400); err != nil {
		glog.Warningf("writing chunk: %s", err)
		return err
	}

	fi, err := os.Stat(filename)
	if err != nil {
		glog.Warningf("stating chunk after write: %s", err)
		return fmt.Errorf("could not stat file after write: %s", err)
	}
	s.chunks.ReplaceOrInsert(Chunk{
		sum:   sha256sum,
		mtime: fi.ModTime().Unix(),
	})
	s.chunkBytes += uint64(len(data))
	localChunks.Set(int64(s.chunks.Len()))
	localChunkBytes.Set(int64(s.chunkBytes))
	return nil
}

// ReleaseChunk deletes a chunk with a given SHA-256 sum
func (s *Drive) ReleaseChunk(sha256sum []byte) error {
	s.Lock()
	defer s.Unlock()

	filename := path.Join(s.config.ChunkParentID, hex.EncodeToString(sha256sum))

	fi, err := os.Stat(filename)
	if err != nil {
		return nil // no such file: our work here is done
	}
	s.chunks.Delete(Chunk{sum: sha256sum, mtime: fi.ModTime().Unix()})
	if err := os.Remove(filename); err != nil {
		glog.Warningf("removed cache entry but not file: %s", err)
		return err
	}
	s.chunkBytes -= uint64(fi.Size())
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

// NewChunkLister returns an iterator which lists the chunks stored on disk.
func (s *Drive) NewChunkLister() drive.ChunkLister {
	var sums [][]byte
	s.Lock()
	defer s.Unlock()
	s.chunks.Ascend(func(item btree.Item) bool {
		sums = append(sums, item.(Chunk).sum)
		return true
	})
	return &ChunkLister{sums: sums}
}

// ChunkLister allows iterating the chunks stored on disk.
type ChunkLister struct {
	sums [][]byte
	ptr  int
}

// Next increments the pointer.
func (c *ChunkLister) Next() bool {
	c.ptr++
	return c.ptr <= len(c.sums)
}

// Sha256 returns the chunk pointed to by the pointer.
func (c *ChunkLister) Sha256() []byte {
	if c.ptr > len(c.sums) {
		return nil
	}
	return c.sums[c.ptr-1]
}

// Err returns precisely no errors.
func (c *ChunkLister) Err() error {
	return nil
}

// cleanup iterates the provided BTree and removes the oldest entries from the
// filesystem, in the provided directory, to bring the length below the
// provided maximum size.  cleanup is called at insert time, so size is Max-1,
// to make space for the new entry being inserted.
func (s *Drive) cleanup(file bool, size uint64) error {
	var bt *btree.BTree
	var dir string
	if file {
		bt = s.files
		dir = s.config.FileParentID
	} else {
		bt = s.chunks
		dir = s.config.ChunkParentID
	}
	for {
		if file {
			len := s.files.Len()
			if len == 0 || uint64(len) <= s.config.MaxFiles-size {
				return nil
			}
		} else {
			if s.chunkBytes <= s.config.MaxChunkBytes-size {
				return nil
			}
		}

		oldest := hex.EncodeToString(bt.Min().(Chunk).sum)
		r := path.Join(dir, oldest)
		if err := os.Remove(r); err != nil {
			return err
		}
		bt.DeleteMin()
		if !file {
			s.chunkBytes -= size
		}
	}
}
