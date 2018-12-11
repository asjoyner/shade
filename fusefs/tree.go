package fusefs

import (
	"encoding/json"
	"errors"
	"expvar"
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
	treeNodes             = expvar.NewInt("treeNodes")
	lastRefreshDurationMs = expvar.NewInt("lastRefreshDurationMs")
)

// Node is a very compact representation of a shade.File.  It can also be used
// to represent a sythetic directory, for tree traversal.
type Node struct {
	// Filename is the complete path to a node, with no leading or trailing
	// slash.
	Filename     string
	Filesize     int64 // in bytes
	ModifiedTime time.Time
	// Deleted indicates the file was Deleted at ModifiedTime.  NodeByPath
	// responds exactly as if the node did not exist.
	Deleted   bool
	Sha256sum []byte // the sha of the full shade.File
	// Children is a map indicating the presence of a node immediately
	// below the current node in the tree.  The key is only the name of that
	// node, a relative path, not fully qualified.
	// TODO(asjoyner): use a struct{} here for efficiency?
	// unsafe.Sizeof indicates it would save 1 byte per FS entry
	Children map[string]bool
	// TODO(asjoyner): update LastSeen each poll, timeout entries so deleted
	// files eventually disappear from Tree.
	// LastSeen time.Time
}

// Synthetic returns true for synthetically created directories.
func (n *Node) Synthetic() bool {
	if n.Sha256sum == nil {
		return true
	}
	return false
}

// Tree is a representation of all files known to the provided drive.Client.
// It initially downlods, then periodically refreshes, the set of files by
// calling ListFiles and GetChunk.  It presents methods to query for what file
// object currently describes what path, and can return a Node or shade.File
// struct representing that node in the tree.
type Tree struct {
	client drive.Client
	nodes  map[string]Node // full path to node
	nm     sync.RWMutex    // protects nodes
	debug  bool
}

// NewTree queries client to discover all the shade.File(s).  It returns a Tree
// object which is ready to answer questions about the nodes in the file tree.
// If the initial query fails, an error is returned instead.
func NewTree(client drive.Client, refresh *time.Ticker) (*Tree, error) {
	t := &Tree{
		client: client,
		nodes: map[string]Node{
			"": {
				Filename: "",
				Children: make(map[string]bool),
			}},
	}
	if err := t.Refresh(); err != nil {
		return nil, fmt.Errorf("initializing Tree: %s", err)
	}
	if refresh != nil {
		go t.periodicRefresh(refresh)
	}
	return t, nil
}

// NodeByPath returns a Node describing the given path.
func (t *Tree) NodeByPath(p string) (Node, error) {
	p = strings.TrimPrefix(p, "/")
	t.nm.RLock()
	defer t.nm.RUnlock()
	n, ok := t.nodes[p]
	if !ok || n.Deleted {
		t.log("known nodes:\n")
		for _, n := range t.nodes {
			t.log(fmt.Sprintf("%+v\n", n))
		}
		return Node{}, fmt.Errorf("no such node: %q", p)
	}
	return n, nil
}

func unmarshalChunk(fj, sha []byte) (*shade.File, error) {
	file := &shade.File{}
	if err := json.Unmarshal(fj, file); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal sha256sum %x: %s", sha, err)
	}
	return file, nil
}

// FileByNode returns the full shade.File object for a given node.
func (t *Tree) FileByNode(n Node) (*shade.File, error) {
	if n.Synthetic() {
		return nil, errors.New("no shade.File defined")
	}
	fj, err := t.client.GetFile(n.Sha256sum)
	if err != nil {
		return nil, fmt.Errorf("GetChunk(%x): %s", n.Sha256sum, err)
	}
	if fj == nil || len(fj) == 0 {
		return nil, fmt.Errorf("Could not find JSON for node: %q", n.Filename)
	}

	f := &shade.File{}
	if err := f.FromJSON(fj); err != nil {
		return nil, err
	}
	return f, nil
}

// HasChild returns true if child exists immediately below parent in the file
// tree.
func (t *Tree) HasChild(parent, child string) bool {
	t.nm.RLock()
	defer t.nm.RUnlock()
	return t.nodes[parent].Children[child]
}

// NumNodes returns the number of nodes (files + synthetic directories) in the
// system.
func (t *Tree) NumNodes() int {
	t.nm.RLock()
	defer t.nm.RUnlock()
	return len(t.nodes)
}

// Mkdir provides a way to create synthetic directories, for the Mkdir Fuse op
func (t *Tree) Mkdir(dir string) Node {
	dir = strings.TrimPrefix(dir, "/")
	t.nm.Lock()
	defer t.nm.Unlock()
	t.nodes[dir] = Node{
		Filename: dir,
		Children: make(map[string]bool),
	}
	t.addParents(dir)
	return t.nodes[dir]
}

// Create adds a new shade.File node to the tree
func (t *Tree) Create(filename string) Node {
	t.nm.Lock()
	defer t.nm.Unlock()
	node := Node{
		Filename:  filename,
		Sha256sum: []byte("f00d"),
	}
	t.nodes[node.Filename] = node
	t.addParents(node.Filename)
	return node
}

// Update accepts a replacement shade.File node
func (t *Tree) Update(n Node) {
	t.nm.Lock()
	defer t.nm.Unlock()
	on, ok := t.nodes[n.Filename]
	if !ok {
		t.log(fmt.Sprintf("Attempt to update a non-existent node: %+v", n))
		return
	}
	if on.ModifiedTime.After(n.ModifiedTime) {
		t.log(fmt.Sprintf("Update mtime (%s) older than current Node (%s)", n.ModifiedTime, on.ModifiedTime))
		return
	}
	t.nodes[n.Filename] = n
	if n.Deleted {
		dir, f := path.Split(n.Filename)
		dir = strings.TrimSuffix(dir, "/")
		parent, ok := t.nodes[dir]
		if !ok {
			t.log(fmt.Sprintf("Updated node without a parent: %+v", n))
			return
		}
		delete(parent.Children, f)
	}
}

// Refresh updates the cached view of the Tree by calling ListFiles and
// processing the result.
func (t *Tree) Refresh() error {
	t.log("Begining cache refresh cycle.")
	start := time.Now()
	// key is a string([]byte) representation of the file's SHA2
	knownNodes := make(map[string]bool)
	newFiles, err := t.client.ListFiles()
	if err != nil {
		return fmt.Errorf("%q ListFiles(): %s", t.client.GetConfig().Provider, err)
	}
	t.log(fmt.Sprintf("Found %d file(s) via %s", len(newFiles), t.client.GetConfig().Provider))
	// fetch all those files into the local disk cache
	for _, sha256sum := range newFiles {
		// check if we have already processed this Node
		if knownNodes[string(sha256sum)] {
			continue // we've already processed this file
		}

		// fetch the file Chunk
		f, err := t.client.GetFile(sha256sum)
		if err != nil {
			// TODO(asjoyner): if !client.Local()... retry?
			log.Printf("Failed to fetch file %x: %s", sha256sum, err)
			continue
		}
		// unmarshal and populate t.nodes as the shade.files go by
		file := &shade.File{}
		if err := file.FromJSON(f); err != nil {
			log.Printf("%v", err)
			continue
		}
		node := Node{
			Filename:     file.Filename,
			Filesize:     file.Filesize,
			ModifiedTime: file.ModifiedTime,
			Deleted:      file.Deleted,
			Sha256sum:    sha256sum,
			Children:     nil,
		}
		t.log(fmt.Sprintf("processing node: %+v", node))
		t.nm.Lock()
		// TODO(asjoyner): handle file + directory collisions
		if existing, ok := t.nodes[node.Filename]; ok && existing.ModifiedTime.After(node.ModifiedTime) {
			t.nm.Unlock()
			continue
		}
		t.nodes[node.Filename] = node
		if node.Deleted { // ensure the parent is updated
			dir, f := path.Split(node.Filename)
			dir = strings.TrimSuffix(dir, "/")
			parent, _ := t.nodes[dir]
			delete(parent.Children, f) // harmless if parent doesn't exist
		} else {
			t.addParents(node.Filename)
		}
		t.nm.Unlock()
		knownNodes[string(sha256sum)] = true
	}
	t.log(fmt.Sprintf("Refresh complete with %d file(s).", len(knownNodes)))
	lastRefreshDurationMs.Set(int64(time.Since(start).Nanoseconds() / 1000))
	treeNodes.Set(int64(len(knownNodes)))
	return nil
}

// recursive function to update parent dirs
func (t *Tree) addParents(filepath string) {
	dir, f := path.Split(filepath)
	dir = strings.TrimSuffix(dir, "/")
	t.log(fmt.Sprintf("adding %q as a child of %q", f, dir))
	// TODO(asjoyner): handle file + directory collisions
	if parent, ok := t.nodes[dir]; !ok {
		// if the parent node doesn't yet exist, initialize it
		t.nodes[dir] = Node{
			Filename: dir,
			Children: map[string]bool{f: true},
		}
	} else {
		parent.Children[f] = true
		return
	}
	if dir != "" {
		t.addParents(dir)
	}
}

func (t *Tree) periodicRefresh(refresh *time.Ticker) {
	for {
		<-refresh.C
		t.Refresh()
	}
}

// Debug causes the client to print helpful message via the log library.
func (t *Tree) Debug() {
	t.debug = true
}

func (t *Tree) log(msg string) {
	if t.debug {
		log.Printf("CACHE: %s", msg)
	}
}
