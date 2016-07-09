package cache

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

var (
	defaultCacheDir = path.Join(shade.ConfigDir(), "cache")
	cacheDir        = flag.String("cache", defaultCacheDir, "Where to store the drive data cache")
)

// A very compact representation of a file
type Node struct {
	Filename     string
	Filesize     uint64
	ModifiedTime time.Time
	FileID       string
	// TODO(asjoyner): use a struct{} here for efficiency?
	Children map[string]bool
}

// Reader is a wrapper around a slice of cloud storage backends.  It presents an
// interface to query for the union of the set of known files by an integer ID,
// which will be stable across single processes invoking this cache, a node
// representing that file, or a single chunk of that file.  It can also cache a
// configurable quantity of chunks to disk.
//
// TODO(asjoyner): implement disk caching of data blocks.
type Reader struct {
	clients []drive.Client
	nodes   map[string]Node // full path to node
	sync.RWMutex
}

func New(clients []drive.Client, t *time.Ticker) (*Reader, error) {
	c := &Reader{clients: clients}
	if err := c.refresh(); err != nil {
		return nil, err
	}
	go c.periodicRefresh(t)
	return c, nil
}

// NodeByPath returns the current file object for a given path
func (c *Reader) NodeByPath(p string) (Node, error) {
	c.RLock()
	defer c.RUnlock()
	if n, ok := c.nodes[p]; ok {
		return n, nil
	}
	return Node{}, errors.New("no such node")
}

func (c *Reader) HasChild(parent, child string) bool {
	c.RLock()
	defer c.RUnlock()
	return c.nodes[parent].Children[child]
}

// NumNodes returns the number of nodes (files + synthetic directories) in the
// system.
func (c *Reader) NumNodes() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.nodes)
}

// refresh updates the cache
func (c *Reader) refresh() error {
	knownNodes := make(map[string]bool)
	for _, client := range c.clients {
		lfm, err := client.ListFiles()
		if err != nil {
			return err
		}
		// fetch all those files into the local disk cache
		for id, sha256sum := range lfm {
			var (
				f    []byte
				file *shade.File
				err  error
			)
			// check if we have already processed this Node
			if knownNodes[string(sha256sum)] {
				continue // we've already processed this file
			}

			// check if the File is already in the disk cache
			f, err = retrieveChunk(sha256sum)
			if err != nil {
				// we have to fetch the file Chunk
				f, err = client.GetFile(id)
				if err != nil {
					// TODO(asjoyner): retry
					log.Printf("Failed to fetch %s with fileId %s: %s", sha256sum, id, err)
					continue // the client did not have the file?
				}
				// store it in the disk cache
				storeChunk(sha256sum, f)
				if err != nil {
					log.Printf("Failed to store checksum %s: %s", sha256sum, err)
				}
			}

			// unmarshal and populate c.nodes as the shade.files go by
			if err := json.Unmarshal(f, file); err != nil {
				continue
			}
			node := Node{
				Filename:     file.Filename,
				Filesize:     uint64(file.Filesize),
				ModifiedTime: file.ModifiedTime,
				FileID:       id,
				Children:     nil,
			}
			c.Lock()
			existing, ok := c.nodes[node.Filename]
			// TODO(asjoyner): handle file + directory collisions
			if ok && existing.ModifiedTime.After(node.ModifiedTime) {
				c.Unlock()
				continue
			}
			c.nodes[node.Filename] = node
			c.addParents(node.Filename)
			c.Unlock()
			knownNodes[string(sha256sum)] = true
		}
	}
	return nil
}

// recursive function to update parent dirs
func (c *Reader) addParents(filepath string) {
	dir, f := path.Split(filepath)
	// if the parent node doesn't yet exist, it is implicitly created here
	// TODO(asjoyner): handle file + directory collisions
	parent := c.nodes[dir]
	parent.Children[f] = true
	if dir != "" {
		c.addParents(strings.TrimSuffix(dir, "/"))
	}
}

func (c *Reader) periodicRefresh(t *time.Ticker) {
	for {
		<-t.C
		c.refresh()
	}
}

func storeChunk(sha256sum []byte, data []byte) error {
	filename := path.Join(*cacheDir, string(sha256sum))
	if err := ioutil.WriteFile(filename, data, 0600); err != nil {
		return err
	}
	return nil
}

func retrieveChunk(sha256sum []byte) ([]byte, error) {
	filename := path.Join(*cacheDir, string(sha256sum))
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return f, nil
}