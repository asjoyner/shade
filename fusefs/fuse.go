package fusefs

// This is a thin layer of glue between the bazil.org/fuse kernel interface
// and the Shade Drive API.
//
// This module uses glog for variable output.  The vmodule levels above 6 get
// very chatty. Roughly, they contain:
//  3. writes of file objects to Fuse
//  4. Drive client reads, refresh timing information
//  5. one log line per "interesting" fuse operation (open, create, stat)
//  6. summary stats on some frequent operations (read, writes of chunks)
//  7. one log line for every operation handled by the fuse API
//  8. raw data as returned by some interesting operations (readdir)
//  9. debugging parameters of tricky internal calculations (offsets, etc)

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"github.com/golang/glog"
	"github.com/jpillora/backoff"
)

var (
	kernelRefresh = flag.Duration("kernel-refresh", time.Minute, "How long the kernel should cache metadata entries.")
	numWorkers    = flag.Int("numFuseWorkers", 20, "The number of goroutines to service fuse requests.")
	maxRetries    = flag.Int("maxRetries", 10, "The number of times to try to write a chunk to persistent storage.")
)

// DefaultChunkSizeBytes defines the default for newly created shade.File(s)
var DefaultChunkSizeBytes = 16 * 1024 * 1024

const blockSize uint32 = 4096

// Server holds the state about the fuse connection
type Server struct {
	client  drive.Client
	tree    *Tree
	inode   *InodeMap
	uid     uint32 // uid of the user who mounted the FS
	gid     uint32 // gid of the user who mounted the FS
	conn    *fuse.Conn
	handles []handle              // index is the handleid, inode=0 if free
	hm      sync.Mutex            // protects access to handles
	writers map[int]io.PipeWriter // index matches fh
}

// New returns a Server which will service fuse requests arriving on conn,
// based on data retrieved from drive.Client.  It is ready to serve requests
// when Server.conn.Ready is closed.  The cached view of files is updated every
// refresh.
func New(client drive.Client, conn *fuse.Conn, refresh *time.Ticker) (*Server, error) {
	tree, err := NewTree(client, refresh)
	if err != nil {
		return nil, err
	}
	uid, gid, err := uidAndGid()
	if err != nil {
		return nil, err
	}
	return &Server{
		client:  client,
		tree:    tree,
		inode:   NewInodeMap(),
		writers: make(map[int]io.PipeWriter),
		conn:    conn,
		uid:     uid,
		gid:     gid,
	}, nil
}

type handle struct {
	inode  fuse.NodeID
	file   *shade.File
	chunks map[int64]shade.Chunk
	dirty  map[int64][]byte
}

// return the current bytes of a chunk
// TODO: write a test for this
func (h *handle) chunkBytes(chunkNum int64, client drive.Client) ([]byte, error) {
	if dirtyChunk, ok := h.dirty[chunkNum]; ok {
		return dirtyChunk, nil
	}
	cleanChunk, ok := h.chunks[chunkNum]
	if !ok { // no chunk data at this offset (yet)
		return make([]byte, 0), nil
	}
	cb, err := client.GetChunk(cleanChunk.Sha256, h.file)
	if err != nil {
		return nil, err
	}
	return cb, nil
}

// applyWrite takes data, at an offset, and applies it to the open file as
// dirty blocks.  It uses the provided client to look up the existing content
// of the associated shade.File object, as necessary.
//
// For some additional notes on how this works, see Chunk Notes.md.
func (h *handle) applyWrite(data []byte, offset int64, client drive.Client) error {
	// determine which chunks need to be updated
	chunkSize := int64(h.file.Chunksize)
	writeSize := int64(len(data))
	eoWrite := offset + writeSize
	firstChunk := offset / chunkSize
	lastChunk := (eoWrite - 1) / chunkSize

	var dataPtr int64 // tracks bytes read from data into chunks
	for cn := firstChunk; cn <= lastChunk; cn++ {
		//glog.V(9).Infof("working chunk %d\n", cn)
		chunkStart := cn * chunkSize // the position of the chunk in the file
		var chunkOffset int64        // the start of the write inside this chunk
		if offset > chunkStart {
			chunkOffset = offset - chunkStart
		}
		cb, err := h.chunkBytes(cn, client)
		if err != nil {
			return err
		}

		//glog.V(9).Infof("before copy: %q\n", cb)
		n := copy(cb[chunkOffset:], data[dataPtr:])
		//glog.V(9).Infof("post copy: %q\n", cb)
		dataPtr += int64(n)
		// determine if we read all of the data, or filled the chunk
		chunkRemainder := chunkSize - int64(len(cb))
		dataRemainder := writeSize - dataPtr
		var appendSize int64
		//glog.V(9).Infof("dataremaidner: %d chunkRemainder: %d\n", dataRemainder, chunkRemainder)
		if dataRemainder > 0 && dataRemainder <= chunkRemainder {
			appendSize = dataRemainder
		} else if dataRemainder > 0 && dataRemainder > chunkRemainder {
			appendSize = chunkRemainder
		}

		// extend cb if necessary
		if appendSize > 0 {
			//glog.V(9).Infof("append[%d:%d] (%q)\n", dataPtr, appendSize, data)
			cb = append(cb, data[dataPtr:dataPtr+appendSize]...)
			dataPtr += appendSize
		}
		//glog.V(9).Infof("post extend: %q\n", cb)

		h.dirty[cn] = cb
	}
	return nil
}

// Serve receives and dispatches Requests from the kernel
func (sc *Server) Serve() error {
	// Create a pool of goroutines to handle incoming Fuse requests
	workRequests := make(chan fuse.Request)
	for w := 1; w <= *numWorkers; w++ {
		go func(reqs chan fuse.Request) {
			for req := range reqs {
				if glog.V(7) {
					glog.Infof("%+v", req)
				}
				sc.serve(req)
			}
			return
		}(workRequests)
	}
	for {
		req, err := sc.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		workRequests <- req
	}
	return nil
}

// Refresh updates the view of the underlying drive.Client.
func (sc *Server) Refresh() error {
	return sc.tree.Refresh()
}

// serve dispatches incoming kernel requests to the appropriate code path
func (sc *Server) serve(req fuse.Request) {
	switch req := req.(type) {
	default:
		// ENOSYS means "this server never implements this request."
		glog.Warningf("ENOSYS: %+v", req)
		req.RespondError(fuse.ENOSYS)

	case *fuse.InitRequest:
		resp := fuse.InitResponse{MaxWrite: 128 * 1024,
			Flags: fuse.InitBigWrites & fuse.InitAsyncRead,
		}
		req.Respond(&resp)

	case *fuse.StatfsRequest:
		resp := &fuse.StatfsResponse{
			Files: uint64(sc.tree.NumNodes()),
			Bsize: blockSize,
		}
		req.Respond(resp)

	case *fuse.GetattrRequest:
		sc.getattr(req)

	case *fuse.LookupRequest:
		sc.lookup(req)

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		req.Respond()

	// Allocate a kernel file handle, return it
	case *fuse.OpenRequest:
		sc.open(req)

	// Silently ignore attempts to change permissions
	case *fuse.SetattrRequest:
		inode := uint64(req.Header.Node)
		p, err := sc.inode.ToPath(uint64(inode))
		if err != nil {
			glog.Warningf("SetattrRequest for inode %d: %s", inode, err)
		}
		n, err := sc.tree.NodeByPath(p)
		if err != nil {
			glog.Warningf("FileByInode(%v): %v", inode, err)
			req.RespondError(fuse.EIO)
			return
		}
		glog.Info("Ignoring Setattr for ", p)
		req.Respond(&fuse.SetattrResponse{Attr: sc.attrFromNode(n, inode)})

	case *fuse.CreateRequest:
		// TODO: if allow_other, require uid == invoking uid to allow writes
		sc.create(req)

	// Return Dirents for directories, or requested portion of file
	case *fuse.ReadRequest:
		if req.Dir {
			sc.readDir(req)
		} else {
			sc.read(req)
		}

	// Return MkdirResponse (it's LookupResponse, essentially) of new dir
	case *fuse.MkdirRequest:
		sc.mkdir(req)

	// Removes the inode described by req.Header.Node
	// Respond() for success, RespondError otherwise
	case *fuse.RemoveRequest:
		sc.remove(req)

	// req.Header.Node describes the current parent directory
	// req.NewDir describes the target directory (may be the same)
	// req.OldName and req.NewName describe any (or no) change in name
	case *fuse.RenameRequest:
		sc.rename(req)

	// Responds with the number of bytes written on success, RespondError otherwise
	case *fuse.WriteRequest:
		sc.write(req)

	// Flush writes to the underlying storage layers
	case *fuse.FlushRequest:
		sc.hm.Lock()
		defer sc.hm.Unlock()
		sc.flush(req.Handle)
		req.Respond()

	// Ack release of the kernel's mapping an inode->fileId
	// This corresponds to a close() on a filehandle
	case *fuse.ReleaseRequest:
		sc.release(req)

	case *fuse.DestroyRequest:
		req.Respond()
	}
}

func (sc *Server) nodeByID(inode fuse.NodeID) (Node, error) {
	filename, err := sc.inode.ToPath(uint64(inode))
	if err != nil {
		return Node{}, fmt.Errorf("ToPath(%d): %v", inode, err)
	}
	n, err := sc.tree.NodeByPath(filename)
	if err != nil {
		return Node{}, fmt.Errorf("NodeByPath(%v): %v", filename, err)
	}
	return n, nil
}

// gettattr returns fuse.Attr for the inode described by req.Header.Node
func (sc *Server) getattr(req *fuse.GetattrRequest) {
	n, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		glog.Warningf("getattr: sc.nodeById(%d): %s", req.Header.Node, err)
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: getattr during upload must return current file size

	resp := &fuse.GetattrResponse{
		Attr: sc.attrFromNode(n, uint64(req.Header.Node)),
	}
	req.Respond(resp)
}

// Return a LookupResponse for the named child of an inode, or ENOENT
func (sc *Server) lookup(req *fuse.LookupRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.LookupResponse{}
	// This request is by inode.  Lookup what filename was assigned to that inode.
	parentDir, err := sc.inode.ToPath(inode)
	if err != nil {
		glog.Warningf("lookup of unassigned inode %d: %s", inode, err)
		req.RespondError(fuse.ENOENT)
		return
	}
	// Get the Node for the child of that inode, if it exists
	filename := strings.TrimPrefix(path.Join(parentDir, req.Name), "/")
	node, err := sc.tree.NodeByPath(filename)
	if err != nil {
		glog.Warningf("Lookup(%v in %v): ENOENT", filename, inode)
		req.RespondError(fuse.ENOENT)
		return
	}
	resp.Node = fuse.NodeID(sc.inode.FromPath(filename))
	resp.EntryValid = *kernelRefresh
	resp.Attr = sc.attrFromNode(node, inode)
	if glog.V(5) {
		glog.Infof("Lookup(%v in %v): %+v", req.Name, inode, resp.Node)
	}
	req.Respond(resp)
}

func (sc *Server) readDir(req *fuse.ReadRequest) {
	resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
	n, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		glog.Warningf("nodeByID(%d): %v", req.Header.Node, err)
		req.RespondError(fuse.EIO)
		return
	}

	// HandleRead requires the data section to be sorted the same way each time,
	// but they are stored in a map.  So read them out and sort them first.
	var children []string
	for name := range n.Children {
		children = append(children, name)
	}
	sort.Strings(children)

	var data []byte
	for _, name := range children {
		childPath := strings.TrimPrefix(path.Join(n.Filename, name), "/")
		c, err := sc.tree.NodeByPath(childPath)
		if err != nil {
			glog.Warningf("child: NodeByPath(%v): %v", childPath, err)
			req.RespondError(fuse.EIO)
			return
		}
		childType := fuse.DT_File
		if c.Synthetic() {
			childType = fuse.DT_Dir
		}
		ci := sc.inode.FromPath(childPath)
		data = fuse.AppendDirent(data, fuse.Dirent{Inode: ci, Name: name, Type: childType})
	}
	if glog.V(8) {
		glog.Info("ReadDir Response: %s", string(data))
	}

	fuseutil.HandleRead(req, resp, data)
	req.Respond(resp)
}

func (sc *Server) read(req *fuse.ReadRequest) {
	h, err := sc.handleByID(req.Handle)
	if err != nil || h.file == nil {
		glog.Warningf("handleByID(%v): %v", req.Handle, err)
		req.RespondError(fuse.ESTALE)
		return
	}
	f := h.file
	if glog.V(6) {
		glog.Infof("Read(name: %s, offset: %d, size: %d)", f.Filename, req.Offset, req.Size)
	}
	chunkSize := int64(f.Chunksize)
	chunkSums, err := chunksForRead(f, req.Offset, int64(req.Size))
	if err != nil {
		glog.Warningf("chunksForRead(): %s", err)
		req.RespondError(fuse.EIO)
		return
	}

	var allTheBytes []byte
	for _, cs := range chunkSums {
		chunk, err := sc.client.GetChunk(cs, f)
		if err != nil {
			glog.Warningf("GetChunk(%x): %v", cs, err)
			req.RespondError(fuse.EIO)
			return
		}
		allTheBytes = append(allTheBytes, chunk...)
	}

	dsize := int64(len(allTheBytes))
	chunkNum := req.Offset / chunkSize
	low := req.Offset - chunkNum*(chunkSize)
	if low < 0 {
		low = 0
	}
	if low > dsize {
		glog.Errorf("too-low chunk calculation error (low:%d, dsize:%d): filename: %s, offset:%d, size:%d, filesize:%d", low, dsize, f.Filename, req.Offset, req.Size, f.Filesize)
		req.RespondError(fuse.EIO)
		return
	}
	high := low + int64(req.Size)
	if high > dsize {
		high = dsize
	}
	d := allTheBytes[low:high]
	resp := &fuse.ReadResponse{Data: d}
	if glog.V(8) {
		glog.Infof("Read resp: %+v %d bytes", resp, len(d))
	}
	req.Respond(resp)

	// After responding to the read request, use this gorouting to consider
	// prefetching the next chunk.  The chunk should be prefetched early (so it
	// has time to complete before the chunk is needed), but not when the very
	// first few bytes of the file are read, so that magic(5) reads (eg. file)
	// and other attempts to identify the file don't cause unnecessary chunk
	// prefetching.  To satisfy, we prefetch whenever the 8096 byte is read.
	if low < 8096 && high > 8096 {
		lastChunkJustRead := chunkSums[len(chunkSums)-1]
		var prefetchChunk int
		for i, c := range f.Chunks {
			if bytes.Equal(c.Sha256, lastChunkJustRead) {
				prefetchChunk = i + 1
			}
		}
		if prefetchChunk > len(f.Chunks) {
			glog.V(3).Info("There is no next chunk to prefetch.")
			return
		}
		cs := f.Chunks[prefetchChunk].Sha256
		glog.V(3).Infof("Prefetching the next chunk: %x", cs)
		_, err := sc.client.GetChunk(cs, f)
		if err != nil {
			glog.Warningf("prefetch GetChunk(%x): %v", cs, err)
		}
	}
}

func (sc *Server) attrFromNode(node Node, i uint64) fuse.Attr {
	attr := fuse.Attr{
		Inode: i,
		Uid:   sc.uid,
		Gid:   sc.gid,
		Mode:  0755,
		Nlink: 1,
	}

	if node.Synthetic() { // it's a synthetic directory
		attr.Mode = os.ModeDir | 0755
		attr.Nlink = uint32(len(node.Children) + 2)
		return attr
	}
	blocks := node.Filesize / int64(blockSize)
	if r := node.Filesize % int64(blockSize); r > 0 {
		blocks++
	}
	attr.Atime = node.ModifiedTime
	attr.Mtime = node.ModifiedTime
	attr.Ctime = node.ModifiedTime
	attr.Crtime = node.ModifiedTime
	attr.Size = uint64(node.Filesize)
	attr.Blocks = uint64(blocks)
	return attr
}

// Allocate a file handle, held by the kernel until Release
func (sc *Server) open(req *fuse.OpenRequest) {
	n, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		req.RespondError(fuse.ENOENT)
		return
	}

	if !req.Flags.IsReadOnly() { // write access requested
		// TODO: if allow_other, require uid == invoking uid to allow writes
	}

	// get the shade.File for the node, stuff it in the Handle
	f, err := sc.tree.FileByNode(n)
	if err != nil && !req.Dir {
		glog.Warningf("FileByNode(%v): %s", n, err)
		req.RespondError(fuse.ENOENT)
		return
	}
	hID := sc.allocHandle(req.Header.Node, f)

	resp := fuse.OpenResponse{Handle: fuse.HandleID(hID)}
	glog.V(5).Infof("Open Response: %+v", resp)
	req.Respond(&resp)
}

// allocate a kernel file handle for the requested inode
func (sc *Server) allocHandle(inode fuse.NodeID, f *shade.File) uint64 {
	var hID uint64
	var found bool
	h := handle{inode: inode, file: f, dirty: make(map[int64][]byte)}
	sc.hm.Lock()
	defer sc.hm.Unlock()
	for i, ch := range sc.handles {
		if ch.inode == 0 {
			hID = uint64(i)
			sc.handles[hID] = h
			found = true
			break
		}
	}
	if !found {
		hID = uint64(len(sc.handles))
		sc.handles = append(sc.handles, h)
	}
	return hID
}

// Lookup a handleID by its NodeID
func (sc *Server) handleByID(id fuse.HandleID) (handle, error) {
	sc.hm.Lock()
	defer sc.hm.Unlock()
	if int(id) >= len(sc.handles) {
		return handle{}, fmt.Errorf("handle %v has not been allocated", id)
	}
	return sc.handles[id], nil
}

// Acknowledge release (eg. close) of file handle by the kernel
func (sc *Server) release(req *fuse.ReleaseRequest) {
	sc.hm.Lock()
	defer sc.hm.Unlock()
	h := sc.handles[req.Handle]
	sc.flush(req.Handle)
	h.inode = 0
	glog.V(5).Infof("release on req.Handle: %+v", req.Handle)
	req.Respond()
}

// Allocate handle, corresponding to kernel filehandle, for writes
func (sc *Server) create(req *fuse.CreateRequest) {
	pn, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		req.RespondError(fuse.ENOENT)
		return
	}
	// create child node
	fn := path.Join(pn.Filename, req.Name)
	n := sc.tree.Create(fn)
	inode := sc.inode.FromPath(fn)
	// create file object
	file := shade.NewFile(fn)
	// create handle
	hID := sc.allocHandle(fuse.NodeID(inode), file)

	// Respond to tell the fuse kernel module about the file
	resp := fuse.CreateResponse{
		// describes the opened handle
		OpenResponse: fuse.OpenResponse{
			Handle: fuse.HandleID(hID),
		},
		// describes the created file
		LookupResponse: fuse.LookupResponse{
			Node:       fuse.NodeID(inode),
			EntryValid: *kernelRefresh,
			Attr:       sc.attrFromNode(n, inode),
		},
	}
	glog.V(5).Infof("Create(%v in %v): %+v", req.Name, pn.Filename, resp)

	req.Respond(&resp)
}

// mkdir create a directory in the tree.  This is very cheap, because in Shade,
// directories are entirey ephemeral concepts, only files are stored remotely.
func (sc *Server) mkdir(req *fuse.MkdirRequest) {
	// TODO: if allow_other, require uid == invoking uid to allow writes
	p, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		req.RespondError(fuse.ENOENT)
		return
	}

	if !p.Synthetic() {
		// TODO: is this right?  we want to return "Not a directory"
		req.RespondError(fuse.EEXIST)
		return
	}
	if p.Children[req.Name] {
		req.RespondError(fuse.EEXIST)
	}

	dir := path.Join(p.Filename, req.Name)
	n := sc.tree.Mkdir(dir)

	inode := sc.inode.FromPath(dir)

	resp := fuse.LookupResponse{
		Node:       fuse.NodeID(inode),
		EntryValid: *kernelRefresh,
		Attr:       sc.attrFromNode(n, inode),
	}
	glog.V(5).Infof("Mkdir(%v): %+v", req.Name, resp)
	req.Respond(&fuse.MkdirResponse{LookupResponse: resp})
}

// Removes the inode described by req.Header.Node (doubles as rmdir)
// This is implemented as publishing a file noted as "deleted" with a higher
// ModifiedTime.
func (sc *Server) remove(req *fuse.RemoveRequest) {
	// TODO: if allow_other, require uid == invoking uid to allow writes
	parentdir, err := sc.inode.ToPath(uint64(req.Header.Node))
	if err != nil {
		glog.Warningf("sc.NodeById(%d): %s", req.Header.Node, err)
		req.RespondError(fuse.ENOENT)
		return
	}
	filename := strings.TrimPrefix(path.Join(parentdir, req.Name), "/")
	// create Deleted File
	f := shade.NewFile(filename)
	f.Deleted = true
	node := Node{
		Filename:     f.Filename,
		ModifiedTime: f.ModifiedTime,
		Deleted:      true,
		Sha256sum:    []byte("deleted"),
	}
	if req.Dir {
		// ensure the are no children of this node
		node, err := sc.tree.NodeByPath(filename)
		if err != nil {
			glog.Warningf("sc.NodeById(%d): %s", req.Header.Node, err)
			req.RespondError(fuse.ENOENT)
		}
		if len(node.Children) == 0 {
			glog.Warningf("sc.NodeById(%d): %s", req.Header.Node, err)
			req.RespondError(syscall.ENOTEMPTY)
		}
		node.Sha256sum = nil
	} else {
		// publish Deleted File
		jm, err := json.Marshal(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not marshal shade.File: %s\n", err)
			os.Exit(1)
		}
		sum := shade.Sum(jm)
		for {
			err := sc.client.PutFile(sum, jm)
			if err != nil {
				glog.Errorf("error storing deleted file %s with sum: %x: %s", filename, sum, err)
				continue
			}
			glog.V(5).Infof("stored file %s with sum: %x", filename, sum)
			break
		}
	}
	// remove Node
	glog.V(5).Infof("sc.tree.Update(..%s..)", f.Filename)
	sc.tree.Update(node)
	req.Respond()
}

// rename renames a file or directory, optionally reparenting it
func (sc *Server) rename(req *fuse.RenameRequest) {
	// TODO(asjoyner): shadeify
	req.RespondError(fuse.ENOSYS)
	/*
		// TODO: if allow_other, require uid == invoking uid to allow writes
		oldParent, err := sc.db.FileByInode(uint64(req.Header.Node))
		if err != nil {
			debug.Printf("can't find the referenced inode: %v", req.Header.Node)
			req.RespondError(fuse.ENOENT)
			return
		}
		var f *drive_db.File
		for _, i := range oldParent.Children {
			c, err := sc.db.FileByInode(uint64(i))
			if err != nil {
				debug.Printf("error iterating child inodes: %v", err)
				continue
			}
			if c.Title == req.OldName {
				f = c
			}
		}
		if f == nil {
			debug.Printf("can't find the old file '%v' in '%v'", req.OldName, oldParent.Title)
			req.RespondError(fuse.ENOENT)
			return
		}

		newParent, err := sc.db.FileByInode(uint64(req.NewDir))
		if err != nil {
			debug.Printf("can't find the new parent by inode: %v", req.NewDir)
			req.RespondError(fuse.ENOENT)
			return
		}

		// did the name change?
		if req.OldName != req.NewName {
			f.Title = req.NewName
		}

		// did the parent change?
		var sameParent bool
		var numParents int
		var oldParentId string
		for _, o := range f.Parents {
			numParents++
			oldParentId = o.Id
			if o.Id == newParent.Id {
				sameParent = true
			}
		}
		if !sameParent && numParents > 1 {
			// TODO: Figure out how to identify which of the multiple parents the
			// file is being moved from, so we can call RemoveParents() correctly
			debug.Printf("can't reparent file with multiple parents: %v", req.OldName)
			req.RespondError(fuse.ENOSYS)
			return
		}

		u := sc.service.Files.Update(f.Id, f.File)
		if !sameParent {
			debug.Printf("moving from %v to %v", oldParentId, newParent.Id)
			u = u.AddParents(newParent.Id)
			u = u.RemoveParents(oldParentId)
		}
		r, err := u.Do()
		if err != nil {
			debug.Printf("failed to update '%v' in drive: %v", req.OldName, err)
			req.RespondError(fuse.EIO)
			return
		}

		if _, err := sc.db.UpdateFile(nil, r); err != nil {
			debug.Printf("failed to update leveldb and cache: ", err)
			req.RespondError(fuse.EIO)
			return
		}
		debug.Printf("rename complete")
		req.Respond()
		return
	*/
}

// Pass sequential writes on to the correct handle for uploading
func (sc *Server) write(req *fuse.WriteRequest) {
	// TODO: if allow_other, require uid == invoking uid to allow writes
	h, err := sc.handleByID(req.Handle)
	if err != nil {
		glog.Warningf("handleByID(%v): %v", req.Handle, err)
		req.RespondError(fuse.ESTALE)
		return
	}

	// update chunks in handle
	sc.hm.Lock()
	defer sc.hm.Unlock()
	if h.applyWrite(req.Data, req.Offset, sc.client); err != nil {
		req.RespondError(fuse.EIO)
		return
	}
	sc.handles[req.Handle] = h
	req.Respond(&fuse.WriteResponse{Size: len(req.Data)})
}

// Write out the dirty chunks to the shade drive.Client
// Nb: caller is responsible for holding sc.hm
func (sc *Server) flush(hID fuse.HandleID) {
	h := sc.handles[hID]
	if h.file == nil || len(h.dirty) == 0 {
		return
	}
	// ensure h.file.Chunks is large enough
	var lastDirtyChunk int64 = -1
	for cn := range h.dirty {
		if cn > lastDirtyChunk {
			lastDirtyChunk = cn
		}
	}
	glog.V(8).Infof("Chunks length before: %+v", len(h.file.Chunks))
	if int64(len(h.file.Chunks)) <= lastDirtyChunk {
		nc := make([]shade.Chunk, lastDirtyChunk+1, lastDirtyChunk+1)
		copy(nc, h.file.Chunks)
		h.file.Chunks = nc
	}
	glog.V(8).Infof("Chunks length: %+v", len(h.file.Chunks))
	glog.V(8).Infof("lastDirtyChunk: %+v", lastDirtyChunk)
	for cn, dirtyChunk := range h.dirty {
		sum := shade.Sum(dirtyChunk)
		h.file.Chunks[cn].Sha256 = sum
		h.file.Chunks[cn].Nonce = shade.NewNonce()
		if cn+1 == int64(len(h.file.Chunks)) {
			h.file.LastChunksize = len(dirtyChunk)
		}
		numRetries := 0
		b := &backoff.Backoff{Factor: 4}
		for {
			numRetries++
			err := sc.client.PutChunk(sum, dirtyChunk, h.file)
			if err != nil {
				glog.Errorf("error storing chunk with sum (retry %d): %x: %s", numRetries, sum, err)
				if numRetries >= *maxRetries {
					glog.Errorf("aborting write, maximum retries exceeded.")
					break
				}
				time.Sleep(b.Duration())
				continue
			}
			if glog.V(6) {
				glog.Infof("stored chunk with sum: %x", sum)
			}
			break
		}
	}
	h.dirty = nil
	h.file.ModifiedTime = time.Now()
	h.file.UpdateFilesize()
	jm, err := json.Marshal(h.file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not marshal shade.File: %s\n", err)
		os.Exit(1)
	}
	sum := shade.Sum(jm)
	for {
		err := sc.client.PutFile(sum, jm)
		if err != nil {
			glog.Errorf("error storing file %s with sum: %x: %s", h.file.Filename, sum, err)
			continue
		}
		glog.V(3).Infof("stored file %s with sum: %x", h.file.Filename, sum)
		break
	}

	// Update sc.tree's understanding of the Node
	n, err := sc.tree.NodeByPath(h.file.Filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not find existing file being flushed: %s\n", err)
	}
	n.Filesize = h.file.Filesize
	n.ModifiedTime = h.file.ModifiedTime
	n.Sha256sum = sum
	sc.tree.Update(n)

	// Update the handle
	sc.handles[hID] = h
}

func chunksForRead(f *shade.File, offset, size int64) ([][]byte, error) {
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("negative offset and size are unsupported")
	}
	chunkSize := int64(f.Chunksize)
	firstChunk := offset / chunkSize
	lastChunk := ((offset + size - 1) / chunkSize) + 1
	if firstChunk > int64(len(f.Chunks)-1) {
		return nil, fmt.Errorf("no first chunk %d for read at %d (%d bytes) in %v", firstChunk, offset, size, f)
	}
	// extract the Sha256 sums from the chunk objects
	var chunks [][]byte
	for i := firstChunk; i < lastChunk; i++ {
		if i > int64(len(f.Chunks)-1) {
			glog.Errorf("no chunk %d for read at %d (%d bytes) in %v", lastChunk, offset, size, f)
			continue
		}
		chunks = append(chunks, f.Chunks[i].Sha256)
	}
	return chunks, nil
}

// uidAndGid returns those values for the process, or err
func uidAndGid() (uint32, uint32, error) {
	userCurrent, err := user.Current()
	if err != nil {
		return 0, 0, err
	}
	uidInt, err := strconv.Atoi(userCurrent.Uid)
	if err != nil {
		return 0, 0, err
	}
	uid := uint32(uidInt)
	gidInt, err := strconv.Atoi(userCurrent.Gid)
	if err != nil {
		return 0, 0, err
	}
	gid := uint32(gidInt)

	return uid, gid, nil
}
