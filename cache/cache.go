package cache

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	cacheDebug      = flag.Bool("cacheDebug", false, "Print cache debugging traces")
)

// Node is a very compact representation of a file
type Node struct {
	Filename     string
	Filesize     uint64
	ModifiedTime time.Time
	Sha256sum    []byte // the sha of the associated File
	// TODO(asjoyner): use a struct{} here for efficiency?
	Children map[string]bool
}

// Synthetic notes whether the node has a shasum.
func (n *Node) Synthetic() bool {
	if n.Sha256sum == nil {
		return true
	}
	return false
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

// NewReader returns a new fully initialized Reader object.
func NewReader(clients []drive.Client, t *time.Ticker) (*Reader, error) {
	c := &Reader{
		clients: clients,
		nodes: map[string]Node{
			"/": {
				Filename: "/",
				Children: make(map[string]bool),
			}},
	}
	if err := c.refresh(); err != nil {
		return nil, fmt.Errorf("initializing cache: %s", err)
	}
	go c.periodicRefresh(t)
	return c, nil
}

// NodeByPath returns the current file object for a given path.
func (c *Reader) NodeByPath(p string) (Node, error) {
	c.RLock()
	defer c.RUnlock()
	if n, ok := c.nodes[p]; ok {
		return n, nil
	}
	// TODO(shanel): Should this be debug?
	log.Printf("%+v\n", c.nodes)
	return Node{}, fmt.Errorf("no such node: %q", p)
}

func unmarshalChunk(fj, sha []byte) (*shade.File, error) {
	file := &shade.File{}
	if err := json.Unmarshal(fj, file); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal sha256sum %x: %s", sha, err)
	}
	return file, nil
}

// FileByNode returns the full shade.File object for a given node.
func (c *Reader) FileByNode(n Node) (*shade.File, error) {
	if n.Synthetic() {
		return nil, errors.New("no shade.File defined")
	}
	var fj []byte
	var err error
	for _, client := range c.clients {
		fj, err = client.GetChunk(n.Sha256sum)
		if err != nil {
			log.Printf("Failed to fetch %s: %s", n.Sha256sum, err)
			continue
		}
	}
	if fj == nil || len(fj) == 0 {
		return nil, fmt.Errorf("Could not find JSON for node: %q", n.Filename)
	}
	return unmarshalChunk(fj, n.Sha256sum)
}

// HasChild returns whether parent has a 'child' node child.
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

// GetChunk is not yet implemented.
func (c *Reader) GetChunk(sha256sum []byte) {
}

// refresh updates the cache
func (c *Reader) refresh() error {
	debug("Begining cache refresh cycle.")
	// key is a string([]byte) representation of the file's SHA2
	knownNodes := make(map[string]bool)
	for _, client := range c.clients {
		lfm, err := client.ListFiles()
		if err != nil {
			return fmt.Errorf("%q ListFiles(): %s", client.GetConfig().Provider, err)
		}
		debug(fmt.Sprintf("Found %d file(s) via %s", len(lfm), client.GetConfig().Provider))
		// fetch all those files into the local disk cache
		for _, sha256sum := range lfm {
			// check if we have already processed this Node
			if knownNodes[string(sha256sum)] {
				continue // we've already processed this file
			}

			// fetch the file Chunk
			f, err := client.GetChunk(sha256sum)
			if err != nil {
				// TODO(asjoyner): if !client.Local()... retry?
				log.Printf("Failed to fetch file %x: %s", sha256sum, err)
				continue
			}
			// ensure this file is known to all the writable clients
			for _, lc := range c.clients {
				if lc.GetConfig().Write {
					if err := lc.PutFile(sha256sum, f); err != nil {
						log.Printf("Failed to store checksum %x in %s: %s", sha256sum, client.GetConfig().Provider, err)
					}
				}
			}

			// unmarshal and populate c.nodes as the shade.files go by
			file, err := unmarshalChunk(f, sha256sum)
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			node := Node{
				Filename:     file.Filename,
				Filesize:     uint64(file.Filesize),
				ModifiedTime: file.ModifiedTime,
				Sha256sum:    sha256sum,
				Children:     nil,
			}
			c.Lock()
			// TODO(asjoyner): handle file + directory collisions
			if existing, ok := c.nodes[node.Filename]; ok && existing.ModifiedTime.After(node.ModifiedTime) {
				c.Unlock()
				continue
			}
			c.nodes[node.Filename] = node
			c.addParents(node.Filename)
			c.Unlock()
			knownNodes[string(sha256sum)] = true
		}
	}
	debug(fmt.Sprintf("Refresh complete with %d file(s).", len(knownNodes)))
	return nil
}

// recursive function to update parent dirs
func (c *Reader) addParents(filepath string) {
	dir, f := path.Split(filepath)
	if dir == "" {
		dir = "/"
	} else {
		dir = strings.TrimSuffix(dir, "/")
	}
	debug(fmt.Sprintf("adding %q as a child of %q", f, dir))
	// TODO(asjoyner): handle file + directory collisions
	if parent, ok := c.nodes[dir]; !ok {
		// if the parent node doesn't yet exist, initialize it
		c.nodes[dir] = Node{
			Filename: dir,
			Children: map[string]bool{f: true},
		}
	} else {
		parent.Children[f] = true
	}
	if dir != "/" {
		c.addParents(dir)
	}
}

func (c *Reader) periodicRefresh(t *time.Ticker) {
	for {
		<-t.C
		c.refresh()
	}
}

func debug(args interface{}) {
	if *cacheDebug {
		log.Printf("CACHE: %s\n", args)
	}
}
