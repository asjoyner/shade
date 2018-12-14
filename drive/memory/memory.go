// Package memory is an in memory storage backend for Shade.
//
// It stores files and chunks transiently in RAM.
// It respects MaxFiles and MaxChukBytes as an LRU cache, evicting the
// least-recently-used file or chunk.  Both Gets and Puts are considered
// "uses", but GetFiles does not update the LRU state of any data.
package memory

import (
	"bytes"
	"errors"
	"expvar"
	"fmt"
	"sync"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	lru "github.com/hashicorp/golang-lru"
)

var (
	memoryFiles      = expvar.NewInt("memoryFiles")
	memoryChunks     = expvar.NewInt("memoryChunks")
	memoryChunkBytes = expvar.NewInt("memoryChunkBytes")
)

// Node is a very compact representation of a shade.File.  It can also be used
func init() {
	drive.RegisterProvider("memory", NewClient)
}

// NewClient returns a Drive client, based on the provided config.
func NewClient(c drive.Config) (drive.Client, error) {
	var err error
	if c.MaxFiles == 0 {
		c.MaxFiles = 50000
	}
	if c.MaxChunkBytes == 0 {
		c.MaxChunkBytes = 1 * 1024 * 1024 * 1024 // 1GB
	}
	client := &Drive{config: c}
	if client.files, err = lru.New(int(c.MaxFiles)); err != nil {
		return nil, fmt.Errorf("initializing file lru: %s", err)
	}
	if client.chunks, err = lru.NewWithEvict(int(c.MaxFiles), client.decrement); err != nil {
		return nil, fmt.Errorf("initializing chunk lru: %s", err)
	}
	return client, nil
}

// Drive implements the drive.Client interface by storing Files and Chunks
// transiently in RAM.  The provided config can be returned, but is otherwise
// ignored.
type Drive struct {
	config     drive.Config
	files      *lru.Cache
	chunks     *lru.Cache
	chunkBytes uint64
	wg         sync.WaitGroup // synchronizes eviction of 'chunks'
}

// ListFiles retrieves all of the File objects known to the client.  The return
// is a list of sha256sums of the file object.  The keys may be passed to
// GetFile() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	keys := s.files.Keys() // returns []interface{}
	resp := make([][]byte, len(keys))
	for i, k := range keys {
		resp[i] = []byte(k.(string))
	}
	return resp, nil
}

// GetFile retrieves a file with a given SHA-256 sum
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	if f, ok := s.files.Get(string(sha256sum)); ok {
		fb := f.([]byte)
		// make a copy, to ensure the caller can't modify the underlying array
		retFile := make([]byte, len(fb))
		copy(retFile, fb)
		return retFile, nil
	}
	return nil, errors.New("not in memory cache")
}

// PutFile writes the metadata describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	s.files.Add(string(sha256sum), f)
	memoryFiles.Set(int64(s.files.Len()))
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256sum []byte, _ *shade.File) ([]byte, error) {
	if c, ok := s.chunks.Get(string(sha256sum)); ok {
		cb := c.([]byte)
		// make a copy, to ensure the caller can't modify the underlying array
		retChunk := make([]byte, len(cb))
		copy(retChunk, cb)
		return retChunk, nil
	}
	return nil, errors.New("chunk not in memory cache")
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte, _ *shade.File) error {
	/*
		fmt.Printf("%d: ", s.chunks.Len())
		for _, k := range s.chunks.Keys() {
			fmt.Printf("%x ", k.(string)[0:3])
		}
		fmt.Println()
		fmt.Printf("adding %x\n", sha256sum)
	*/
	if !s.chunks.Contains(string(sha256sum)) {
		s.chunkBytes += uint64(len(chunk))
	}
	for s.chunkBytes > s.config.MaxChunkBytes {
		//fmt.Printf("%d > %d\n", s.chunkBytes, s.config.MaxChunkBytes)
		s.wg.Add(1)
		s.chunks.RemoveOldest()
		s.wg.Wait()
	}
	//fmt.Printf("adding %x to LRU...\n", sha256sum)
	s.chunks.Add(string(sha256sum), chunk)
	memoryChunks.Set(int64(s.chunks.Len()))
	memoryChunkBytes.Set(int64(s.chunkBytes))
	return nil
}

func (s *Drive) decrement(key interface{}, value interface{}) {
	//fmt.Printf("removing %x\n", key)
	s.chunkBytes -= uint64(len(value.([]byte)))
	s.wg.Done()
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether the storage is local to this machine.
func (s *Drive) Local() bool { return true }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return false }

// Equal compares one Drive instance to another.
func (s *Drive) Equal(other *Drive) error {
	if err := compareLRU(s.files, other.files); err != nil {
		return fmt.Errorf("backing client has different files: %s", err)
	}
	if err := compareLRU(s.chunks, other.chunks); err != nil {
		return fmt.Errorf("backing client has different chunks: %s", err)
	}
	return nil
}

// compareLRU compares two lru.Cache instances.  If they have the same
// contents, they are considered equal.
// The lru.Cache must contain string keys and []byte values.
func compareLRU(a, b *lru.Cache) error {
	for _, k := range a.Keys() {
		bv, ok := b.Peek(k)
		if !ok {
			return fmt.Errorf("second client does not contain key %x", k)
		}
		av, ok := a.Peek(k)
		if !bytes.Equal(av.([]byte), bv.([]byte)) {
			return fmt.Errorf("client have different values for key %x (%x vs. %x)", k, av, bv)
		}
	}
	for _, k := range b.Keys() {
		if !a.Contains(k) {
			return fmt.Errorf("second client does not contain key %x", k)
		}
	}
	return nil
}
