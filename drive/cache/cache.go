// Package cache is an interface to multiple storage backends for Shade.  It
// centralizes the implementation of reading and writing to multiple
// drive.Clients.
package cache

import (
	"errors"
	"fmt"
	"log"

	"github.com/asjoyner/shade/drive"
)

// cache does not call RegisterProvider because it cannot be specified in a
// config.  It is invoked directly by the tools to manage talking to the
// configured clients.

// NewClient returns a Drive client which centralizes reading and writing to
// multiple Providers.
func NewClient(children []drive.Client) (*Drive, error) {
	if len(children) == 0 {
		return nil, errors.New("no clients provided")
	}
	d := &Drive{}
	for _, client := range children {
		if client.GetConfig().Write {
			d.config.Write = true
		}
		d.clients = append(d.clients, client)
	}
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
		go func(client drive.Client) {
			f, err := client.ListFiles()
			if err != nil {
				s.log(fmt.Sprintf("Error reading from %q: %s", client.GetConfig().Provider, err))
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
			if err := client.PutFile(sha256sum, f); err != nil {
				s.log(fmt.Sprintf("%s.PutFile(%x) failed: %s", client.GetConfig().Provider, sha256sum, err))
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
	for _ = range s.clients {
		select {
		case <-persisted:
			return nil
		case <-done:
		}
	}
	return fmt.Errorf("persistent storage configured, but all writes failed: %x", sha256sum)
}

// GetChunk retrieves a chunk with a given SHA-256 sum.  It will be returned
// from the first client in the slice of structs that returns the chunk.
func (s *Drive) GetChunk(sha256sum []byte) ([]byte, error) {
	// TODO(asjoyner): consider adding the ability to cancel GetChunk, then
	// paralellize this with a slight delay between launching each request.
	for _, client := range s.clients {
		chunk, err := client.GetChunk(sha256sum)
		if err != nil {
			continue
		}
		return chunk, nil
	}
	return nil, errors.New("chunk not found")
}

// PutChunk writes a chunk associated with a SHA-256 sum.  It will attempt to write to
// all shade backends configured to Write.  If any backends are Persistent, it
// returns an error if all Persistent backends fail to write.
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte) error {
	if s.config.Write == false {
		return errors.New("no clients configured to write")
	}

	persisted := make(chan struct{}, len(s.clients))
	done := make(chan struct{}, len(s.clients))
	for _, client := range s.clients {
		go func(client drive.Client) {
			if err := client.PutChunk(sha256sum, chunk); err != nil {
				s.log(fmt.Sprintf("%s.PutChunk(%x) failed: %s", client.GetConfig().Provider, sha256sum, err))
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
	for _ = range s.clients {
		select {
		case <-persisted:
			return nil
		case <-done:
		}
	}
	return fmt.Errorf("persistent storage configured, but all writes failed: %x", sha256sum)
}

// GetConfig returns the config used to initialize this client.
func (s *Drive) GetConfig() drive.Config {
	return drive.Config{Provider: "cache"}
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

// Debug enables debug statements to STDERR for non-critical failures to read or
// write from clients.
func (s *Drive) Debug() {
	s.debug = true
}

func (s *Drive) log(output string) {
	if s.debug {
		log.Printf("drive.Cache: %s\n", output)
	}
}
