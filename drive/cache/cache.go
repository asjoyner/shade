// Package cache is an interface to multiple storage backends for Shade.  It
// centralizes the implementation of reading and writing to multiple
// drive.Clients.
package cache

import (
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

var (
	cacheDebug = flag.Bool("cacheDebug", false, "Print cache debugging traces")
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
	d := &Drive{}
	for _, conf := range c.Children {
		child, err := drive.NewClient(conf)
		if err != nil {
			return nil, fmt.Errorf("%s: %s", conf.Provider, err)
		}
		if child.GetConfig().Write {
			d.config.Write = true
		}
		d.clients = append(d.clients, child)
	}
	d.files = make(chan refreshReq, 100)
	go func(d *Drive) {
		for {
			select {
			case r := <-d.files:
				d.refreshFile(r.sha256sum, r.content)
			}
		}
	}(d)
	d.chunks = make(chan refreshReq, 100)
	go func(d *Drive) {
		for {
			select {
			case r := <-d.chunks:
				d.refreshChunk(r.sha256sum, r.content, r.f)
			}
		}
	}(d)
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
	chunks  chan refreshReq
	files   chan refreshReq
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

// GetFile retrieves a file with a given SHA-256 sum.  It will be returned
// from the first client in the slice of structs that returns the chunk.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	for _, client := range s.clients {
		file, err := client.GetFile(sha256sum)
		if err != nil {
			s.log(fmt.Sprintf("File %x not found in %q: %s", sha256sum, client.GetConfig().Provider, err))
			continue
		}
		s.files <- refreshReq{sha256sum: sha256sum, content: file, f: nil}
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
	for range s.clients {
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
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
	// TODO(asjoyner): consider adding the ability to cancel GetChunk, then
	// paralellize this with a slight delay between launching each request.
	for _, client := range s.clients {
		chunk, err := client.GetChunk(sha256sum, f)
		if err != nil {
			s.log(fmt.Sprintf("Chunk %x not found in %q: %s", sha256sum, client.GetConfig().Provider, err))
			continue
		}
		s.files <- refreshReq{sha256sum: sha256sum, content: chunk, f: f}
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
			if err := client.PutChunk(sha256sum, chunk, f); err != nil {
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
	for range s.clients {
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
	flag.Set("cacheDebug", "true")
}

func (s *Drive) log(output string) {
	if *cacheDebug {
		log.Printf("drive.Cache: %s\n", output)
	}
}

func (s *Drive) refreshWorker() {
	select {
	case r := <-s.files:
		s.refreshFile(r.sha256sum, r.content)
	case r := <-s.chunks:
		s.refreshChunk(r.sha256sum, r.content, r.f)
	}
}

// refreshFile calls PutFile on each client which is Local()
// This populates eg. memory and disk clients with files that are
// fetched from remote clients.  Errors are logged, but not returned.
func (s *Drive) refreshFile(sha256sum, file []byte) {
	for _, client := range s.clients {
		if client.Local() {
			client.PutFile(sha256sum, file)
		}
	}
}

// refreshChunk calls PutChunk on each client which is Local()
// This populates eg. memory and disk clients with chunks that are
// fetched from remote clients.  Errors are logged, but not returned.
func (s *Drive) refreshChunk(sha256sum, chunk []byte, f *shade.File) {
	for _, client := range s.clients {
		if client.Local() {
			client.PutChunk(sha256sum, chunk, f)
		}
	}
}
