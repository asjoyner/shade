// Package cache is an interface to multiple storage backends for Shade.  It
// centralizes the implementation of reading and writing to multiple
// drive.Clients.
package cache

import (
	"errors"
	"fmt"

	"github.com/golang/glog"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

func init() {
	drive.RegisterProvider("cache", NewClient)
}

type refreshReq struct {
	sha256sum []byte
	content   []byte
	f         *shade.File
}

// NewClient returns a Drive client which centralizes reading and writing to
// multiple Providers.
func NewClient(c drive.Config) (drive.Client, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("no clients provided")
	}
	d := &Drive{config: c}
	for _, conf := range c.Children {
		child, err := drive.NewClient(conf)
		if err != nil {
			return nil, fmt.Errorf("%s: %s", conf.Provider, err)
		}
		if child.GetConfig().Write {
			glog.V(2).Infof("child %s is writable.", conf.Provider)
			d.config.Write = true
		} else {
			glog.V(2).Infof("child %s is NOT writable.", conf.Provider)
		}
		d.clients = append(d.clients, child)
	}
	glog.V(2).Infof("my final write status is: %v", d.config.Write)
	return d, nil
}

// Drive implements the drive.Client interface by reading and writing to the
// slice of drive.Client interfaces it was provided.  It can return a config
// which describes only its name.
//
// If any of its clients are not Local(), it reports itself as not Local() by
// returning false.  If any of its clients are Persistent(), it requires writes
// to at least one of those backends to succeed, and reports itself as
// Persistent().
type Drive struct {
	config  drive.Config
	clients []drive.Client
	debug   bool
}

// ListFiles retrieves all of the File objects known to all of the provided
// clients.  The return is a list of sha256sums of the file object.  The keys
// may be passed to GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	c := make(chan [][]byte, len(s.clients))
	for _, client := range s.clients {
		// TODO: spawn goroutines for this in advance, one per client?
		// careful to keep it threadsafe
		go func(client drive.Client) {
			f, err := client.ListFiles()
			if err != nil {
				glog.Warningf("error reading from %q: %s", client.GetConfig().Provider, err)
			}
			c <- f
		}(client)
	}

	var resp [][]byte
	for i := 0; i < len(s.clients); i++ {
		resp = append(resp, <-c...)
	}

	return resp, nil
}

// GetFile retrieves a file with a given SHA-256 sum.  It will be returned
// from the first client in the slice of structs that returns the chunk.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	for _, client := range s.clients {
		file, err := client.GetFile(sha256sum)
		if err != nil {
			glog.V(2).Infof("File %x not found in %q: %s", sha256sum, client.GetConfig().Provider, err)
			continue
		}
		for _, c := range s.clients {
			if c.Local() && c != client {
				c.PutFile(sha256sum, file)
			}
		}
		return file, nil
	}
	return nil, errors.New("file not found")
}

// PutFile writes the metadata describing a new file.  It will be written to
// all shade backends configured to Write.  If any backends are Persistent, it
// returns an error if all Persistent backends fail to write.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, f []byte) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}

	persisted := make(chan struct{}, len(s.clients))
	done := make(chan struct{}, len(s.clients))
	for _, client := range s.clients {
		go func(client drive.Client) {
			glog.V(3).Infof("client %s putting file %x", client.GetConfig().Provider, sha256sum)
			if err := client.PutFile(sha256sum, f); err != nil {
				glog.Warningf("%s.PutFile(%x) failed: %s", client.GetConfig().Provider, sha256sum, err)
				done <- struct{}{}
				return
			}
			if !s.Persistent() || client.Persistent() {
				persisted <- struct{}{}
				return
			}
			done <- struct{}{}
		}(client)
	}
	for range s.clients {
		select {
		case <-persisted:
			return nil
		case <-done:
		}
	}
	return fmt.Errorf("persistent storage configured, but all writes failed: %x", sha256sum)
}

// ReleaseFile calls ReleaseFile on each of the provided clients in sequence.
// No errors are returned from child clients.
func (s *Drive) ReleaseFile(sha256sum []byte) error {
	for _, client := range s.clients {
		if err := client.ReleaseFile(sha256sum); err != nil {
			glog.Infof("could not ReleaseFile in %s: %s", client.GetConfig().Provider, err)
		}
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum.  It will be returned
// from the first client in the slice of structs that returns the chunk.
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
	// TODO(asjoyner): consider adding the ability to cancel GetChunk, then
	// paralellize this with a slight delay between launching each request.
	for _, client := range s.clients {
		chunk, err := client.GetChunk(sha256sum, f)
		if err != nil {
			glog.V(2).Infof("Chunk %x not found in %q: %s", sha256sum, client.GetConfig().Provider, err)
			continue
		}
		for _, c := range s.clients {
			if c.Local() {
				glog.V(7).Infof("refreshing chunk %x", sha256sum)
				c.PutChunk(sha256sum, chunk, f)
			}
		}
		return chunk, nil
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk associated with a SHA-256 sum.  It will attempt to write to
// all shade backends configured to Write.  If any backends are Persistent, it
// returns an error if all Persistent backends fail to write.
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte, f *shade.File) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}

	persisted := make(chan struct{}, len(s.clients))
	done := make(chan struct{}, len(s.clients))
	for _, client := range s.clients {
		go func(client drive.Client) {
			glog.V(3).Infof("client %s putting chunk %x", client.GetConfig().Provider, sha256sum)
			if err := client.PutChunk(sha256sum, chunk, f); err != nil {
				glog.Warningf("%s.PutChunk(%x) failed: %s", client.GetConfig().Provider, sha256sum, err)
				done <- struct{}{}
				return
			}
			if !s.Persistent() || client.Persistent() {
				persisted <- struct{}{}
				return
			}
			done <- struct{}{}
		}(client)
	}
	for range s.clients {
		select {
		case <-persisted:
			return nil
		case <-done:
		}
	}
	return fmt.Errorf("persistent storage configured, but all writes failed: %x", sha256sum)
}

// ReleaseChunk calls ReleaseChunk on each of the provided clients in sequence.
// No errors are returned from child clients.
func (s *Drive) ReleaseChunk(sha256sum []byte) error {
	for _, client := range s.clients {
		if err := client.ReleaseChunk(sha256sum); err != nil {
			glog.Infof("could not ReleaseChunk in %s: %s", client.GetConfig().Provider, err)
		}
	}
	return nil
}

// Warm is passed along to each client that is not Local().
func (s *Drive) Warm(chunks [][]byte, f *shade.File) {
	for _, c := range s.clients {
		if !c.Local() {
			c.Warm(chunks, f)
		}
	}
	return
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns true only if all configured storage backends are local to this
// machine.
func (s *Drive) Local() bool {
	for _, c := range s.clients {
		if !c.Local() {
			return false
		}
	}
	return true
}

// Persistent returns true if at least one configured storage backend is
// Persistent().
func (s *Drive) Persistent() bool {
	for _, c := range s.clients {
		if c.Persistent() {
			return true
		}
	}
	return false
}

// NewChunkLister returns an iterator which will return all of the chunks known
// to all child clients.
func (s *Drive) NewChunkLister() drive.ChunkLister {
	c := &ChunkLister{listers: make([]drive.ChunkLister, 0, len(s.clients))}
	for _, client := range s.clients {
		c.listers = append(c.listers, client.NewChunkLister())
	}
	return c
}

// ChunkLister allows iterating the chunks in all child clients.
type ChunkLister struct {
	listers []drive.ChunkLister
	sha256  []byte
	err     error
}

// Next advances the iterator returned by Sha256.
//
// It attempts to see if the current client has another Sha256 to provide.
// When a client is exhausted it advances to the next client.  If an Err is
// encountered, iteration stops and Err() is propagated back to the caller.
func (c *ChunkLister) Next() bool {
	if len(c.listers) == 0 {
		return false // we've iterated all the clients
	}

	// Process the first client in the list
	if c.listers[0].Next() {
		c.sha256 = c.listers[0].Sha256()
		return true
	}
	c.err = c.listers[0].Err()
	if c.err != nil {
		return false
	}

	// client[0] has been exhausted without error, are there more?
	if len(c.listers) > 1 {
		c.listers = c.listers[1:]
		return true
	}
	c.listers = nil
	return false
}

// Sha256 returns the chunk pointed to by the pointer.
func (c *ChunkLister) Sha256() []byte {
	return c.sha256
}

// Err returns the error encountered, if any.
func (c *ChunkLister) Err() error {
	return c.err
}

// refreshFile calls PutFile on each client which is Local()
// This populates eg. memory and disk clients with files that are
// fetched from remote clients.  Errors are logged, but not returned.
func (s *Drive) refreshFile(sha256sum, file []byte) {
}
