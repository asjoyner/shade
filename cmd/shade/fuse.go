package main

// This is a thin layer of glue between the bazil.org/fuse kernel interface
// and the Shade Drive API.

import (
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"path"
	"sync"
	"time"

	"bazil.org/fuse"
	_ "bazil.org/fuse/fs/fstestutil"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"

	"github.com/asjoyner/shade/cache"
)

var kernelRefresh = flag.Duration("kernel-refresh", time.Minute, "How long the kernel should cache metadata entries.")

const blockSize uint32 = 4096

// serveConn holds the state about the fuse connection
type serveConn struct {
	//db         *drive_db.DriveDB
	//service    *drive.Service
	cache   *cache.Reader
	inode   inodeMap
	files   []*shade.File
	clients []drive.Client
	uid     uint32 // uid of the user who mounted the FS
	gid     uint32 // gid of the user who mounted the FS
	conn    *fuse.Conn
	handles []handle              // index is the handleid, inode=0 if free
	writers map[int]io.PipeWriter // index matches fh
	sync.Mutex
}

func NewFuseServer(r *cache.Reader, uid, gid uint32, conn *fuse.Conn) *serveConn {
	return &serveConn{
		cache:   r,
		uid:     uid,
		gid:     gid,
		writers: make(map[int]io.PipeWriter),
		conn:    conn,
	}
}

type handle struct {
	inode fuse.NodeID
	file  shade.File
	// TODO(asjoyner): track dirty chunks here?
	writer   *io.PipeWriter
	lastByte int64
}

// Serve receives and dispatches Requests from the kernel
func (sc *serveConn) Serve() error {
	for {
		req, err := sc.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fuse.Debug(fmt.Sprintf("%+v", req))
		go sc.serve(req)
	}
	return nil
}

// serve dispatches incoming kernel requests to the appropriate code path
func (sc *serveConn) serve(req fuse.Request) {
	switch req := req.(type) {
	default:
		// ENOSYS means "this server never implements this request."
		fuse.Debug(fmt.Sprintf("ENOSYS: %+v", req))
		req.RespondError(fuse.ENOSYS)

	case *fuse.InitRequest:
		resp := fuse.InitResponse{MaxWrite: 128 * 1024,
			Flags: fuse.InitBigWrites & fuse.InitAsyncRead,
		}
		req.Respond(&resp)

	case *fuse.StatfsRequest:
		req.Respond(
			&fuse.StatfsResponse{
				Files: uint64(sc.cache.NumNodes()),
				Bsize: blockSize,
			},
		)

	case *fuse.GetattrRequest:
		sc.getattr(req)

	case *fuse.LookupRequest:
		sc.lookup(req)

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		sc.inode.Release(req.N)
		req.Respond()

	// Allocate a kernel file handle, return it
	case *fuse.OpenRequest:
		sc.open(req)

	// Silently ignore attempts to change permissions
	case *fuse.SetattrRequest:
		inode := uint64(req.Header.Node)
		p, err := sc.inode.ToPath(uint64(inode))
		if err != nil {
			fuse.Debug(fmt.Sprintf("inode %d: %s", inode, err))
		}
		n, err := sc.cache.NodeByPath(p)
		if err != nil {
			fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", inode, err))
			req.RespondError(fuse.EIO)
			return
		}
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

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.FlushRequest:
		req.Respond()

	// Ack release of the kernel's mapping an inode->fileId
	// This corresponds to a close() on a filehandle
	case *fuse.ReleaseRequest:
		sc.release(req)

	case *fuse.DestroyRequest:
		req.Respond()
	}
}

func (sc *serveConn) nodeByID(inode fuse.NodeID) (cache.Node, error) {
	filename, err := sc.inode.ToPath(uint64(inode))
	if err != nil {
		return cache.Node{}, fmt.Errorf("ToPath(%d): %v", inode, err)
	}
	n, err := sc.cache.NodeByPath(filename)
	if err != nil {
		return cache.Node{}, fmt.Errorf("NodeByPath(%v): %v", filename, err)
	}
	return n, nil
}

// gettattr returns fuse.Attr for the inode described by req.Header.Node
func (sc *serveConn) getattr(req *fuse.GetattrRequest) {
	n, err := sc.nodeByID(req.Header.Node)
	if err != nil {
		fuse.Debug(err.Error())
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: getattr during upload must return current file size

	resp := &fuse.GetattrResponse{
		Attr: sc.attrFromNode(n, uint64(req.Header.Node)),
	}
	fuse.Debug(resp)
	req.Respond(resp)
}

// Return a LookupResponse for the named child of an inode, or ENOENT
func (sc *serveConn) lookup(req *fuse.LookupRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.LookupResponse{}
	// This req is by inode.  Lookup what filename was assigned to that inode.
	parentDir, err := sc.inode.ToPath(inode)
	if err != nil {
		fuse.Debug(fmt.Sprintf("lookup unassigned inode %d: %s", inode, err))
		req.RespondError(fuse.ENOENT)
		return
	}
	// Get the cache.Node for the child of that inode, if it exists
	filename := path.Join(parentDir, req.Name)
	node, err := sc.cache.NodeByPath(filename)
	if err != nil {
		fuse.Debug(fmt.Sprintf("Lookup(%v in %v): ENOENT", req.Name, inode))
		req.RespondError(fuse.ENOENT)
		return
	}
	//if node.FileID != "" {  // it's a file
	resp.Node = fuse.NodeID(sc.inode.FromPath(filename))
	resp.EntryValid = *kernelRefresh
	resp.Attr = sc.attrFromNode(node, inode)
	fuse.Debug(fmt.Sprintf("Lookup(%v in %v): %v", req.Name, inode, resp.Node))
	req.Respond(resp)
	return
}

func (sc *serveConn) readDir(req *fuse.ReadRequest) {
	return
	// TODO(asjoyner): shadeify
	/*
		inode := uint64(req.Header.Node)
		resp := &fuse.ReadResponse{make([]byte, 0, req.Size)}
		var dirs []fuse.Dirent
		file, err := sc.db.FileByInode(inode)
		if err != nil {
			fuse.Debug(fmt.Sprintf("FileByInode(%d): %v", inode, err))
			req.RespondError(fuse.EIO)
			return
		}

		for _, inode := range file.Children {
			f, err := sc.db.FileByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("child: FileByInode(%d): %v", inode, err))
				req.RespondError(fuse.EIO)
				return
			}
			childType := fuse.DT_File
			if f.MimeType == driveFolderMimeType {
				childType = fuse.DT_Dir
			}
			dirs = append(dirs, fuse.Dirent{Inode: f.Inode, Name: f.Title, Type: childType})
		}
		fuse.Debug(fmt.Sprintf("%+v", dirs))
		var data []byte
		for _, dir := range dirs {
			data = fuse.AppendDirent(data, dir)
		}
		fuseutil.HandleRead(req, resp, data)
		req.Respond(resp)
	*/
}

func (sc *serveConn) read(req *fuse.ReadRequest) {
	return
	// TODO(asjoyner): shadeify
	/*
		inode := uint64(req.Header.Node)
		resp := &fuse.ReadResponse{}
		// Lookup which fileId this request refers to
		f, err := sc.db.FileByInode(inode)
		if err != nil {
			debug.Printf("FileByInode(%d): %v", inode, err)
			req.RespondError(fuse.EIO)
			return
		}
		debug.Printf("Read(title: %s, offset: %d, size: %d)\n", f.Title, req.Offset, req.Size)
		resp.Data, err = sc.db.ReadFiledata(f.Id, req.Offset, int64(req.Size), f.Filesize)
		if err != nil && err != io.EOF {
			debug.Printf("db.ReadFileData (..%v..): %v", req.Offset, err)
			req.RespondError(fuse.EIO)
			return
		}
		req.Respond(resp)
	*/
}

func (sc *serveConn) attrFromNode(node cache.Node, i uint64) fuse.Attr {
	attr := fuse.Attr{
		Inode: i,
		Uid:   sc.uid,
		Gid:   sc.gid,
		Mode:  0755,
		Nlink: 1,
	}
	if node.FileID == "" { // it's a synthetic directory
		attr.Mode = os.ModeDir | 0755
		attr.Nlink = uint32(len(node.Children) + 2)
		return attr
	}
	blocks := node.Filesize / uint64(blockSize)
	if r := node.Filesize % uint64(blockSize); r > 0 {
		blocks += 1
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
func (sc *serveConn) open(req *fuse.OpenRequest) {
	return
	// TODO(asjoyner): shadeify
	/*
		// This will be cheap, Lookup always preceeds Open, so the cache is warm
		f, err := sc.cache.NodeByPath(sc.inode.ToPath(uint64(req.Header.Node)))
		if err != nil {
			req.RespondError(fuse.ENOENT)
			return
		}

		var hId uint64
		if !req.Flags.IsReadOnly() { // write access requested
			if *readOnly {
				// TODO: if allow_other, require uid == invoking uid to allow writes
				req.RespondError(fuse.EPERM)
				return
			}

			r, w := io.Pipe() // plumbing between WriteRequest and Drive
			go sc.updateInDrive(f.File, r)
			hId = sc.allocHandle(req.Header.Node, f, w)
		} else {
			hId = sc.allocHandle(req.Header.Node, f, nil)
		}

		resp := fuse.OpenResponse{Handle: fuse.HandleID(hId)}
		fuse.Debug(fmt.Sprintf("Open Response: %+v", resp))
		req.Respond(&resp)
	*/
}

// allocate a kernel file handle for the requested inode
func (sc *serveConn) allocHandle(inode fuse.NodeID, w *io.PipeWriter) uint64 {
	var hId uint64
	var found bool
	h := handle{inode: inode, writer: w}
	sc.Lock()
	defer sc.Unlock()
	for i, ch := range sc.handles {
		if ch.inode == 0 {
			hId = uint64(i)
			sc.handles[hId] = h
			found = true
			break
		}
	}
	if !found {
		hId = uint64(len(sc.handles))
		sc.handles = append(sc.handles, h)
	}
	return hId
}

// Lookup a handleID by its NodeID
func (sc *serveConn) handleById(id fuse.HandleID) (handle, error) {
	sc.Lock()
	defer sc.Unlock()
	if int(id) >= len(sc.handles) {
		return handle{}, fmt.Errorf("handle %v has not been allocated", id)
	}
	return sc.handles[id], nil
}

// Acknowledge release (eg. close) of file handle by the kernel
// TODO(asjoyner): flush any remaining dirty blocks?
func (sc *serveConn) release(req *fuse.ReleaseRequest) {
	sc.Lock()
	defer sc.Unlock()
	h := sc.handles[req.Handle]
	if h.writer != nil {
		h.writer.Close()
	}
	h.inode = 0
	req.Respond()
}

// Create file in drive, allocate kernel filehandle for writes
func (sc *serveConn) create(req *fuse.CreateRequest) {
	if *readOnly && !req.Flags.IsReadOnly() {
		req.RespondError(fuse.EPERM)
		return
	}

	// TODO(asjoyner): shadeify
	/*
		pInode := uint64(req.Header.Node)
		parent, err := sc.db.FileByInode(pInode)
		if err != nil {
			debug.Printf("failed to get parent file: %v", err)
			req.RespondError(fuse.EIO)
			return
		}
		p := &drive.ParentReference{Id: parent.Id}

		f := &drive.File{Title: req.Name}
		f.Parents = []*drive.ParentReference{p}
		f, err = sc.service.Files.Insert(f).Do()
		if err != nil {
			debug.Printf("Files.Insert(f).Do(): %v", err)
			req.RespondError(fuse.EIO)
			return
		}
		inode, err := sc.db.InodeForFileId(f.Id)
		if err != nil {
			debug.Printf("failed creating inode for %v: %v", req.Name, err)
			req.RespondError(fuse.EIO)
			return
		}

		r, w := io.Pipe() // plumbing between WriteRequest and Drive
		h := sc.allocHandle(fuse.NodeID(inode), w)

		go sc.updateInDrive(f, r)

		// Tell fuse and the OS about the file
		df, err := sc.db.UpdateFile(nil, f)
		if err != nil {
			debug.Printf("failed to update levelDB for %v: %v", f.Id, err)
			// The write has happened to drive, but we failed to update the kernel.
			// The Changes API will update Fuse, and when the kernel metadata for
			// the parent directory expires, the new file will become visible.
			req.RespondError(fuse.EIO)
			return
		}

		resp := fuse.CreateResponse{
			// describes the opened handle
			OpenResponse: fuse.OpenResponse{
				Handle: fuse.HandleID(h),
				Flags:  fuse.OpenNonSeekable,
			},
			// describes the created file
			LookupResponse: fuse.LookupResponse{
				Node:       fuse.NodeID(inode),
				EntryValid: *kernelRefresh,
				Attr:       sc.attrFromNode(*df, inode),
			},
		}
		fuse.Debug(fmt.Sprintf("Create(%v in %v): %+v", req.Name, parent.Title, resp))

		req.Respond(&resp)
	*/
}

func (sc *serveConn) mkdir(req *fuse.MkdirRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO(asjoyner): shadeify
	/*
		// TODO: if allow_other, require uid == invoking uid to allow writes
		pInode := uint64(req.Header.Node)
		pId, err := sc.db.FileIdForInode(pInode)
		if err != nil {
			debug.Printf("failed to get parent fileid: %v", err)
			req.RespondError(fuse.EIO)
			return
		}
		p := []*drive.ParentReference{&drive.ParentReference{Id: pId}}
		file := &drive.File{Title: req.Name, MimeType: driveFolderMimeType, Parents: p}
		file, err = sc.service.Files.Insert(file).Do()
		if err != nil {
			debug.Printf("Insert failed: %v", err)
			req.RespondError(fuse.EIO)
			return
		}
		debug.Printf("Child of %v created in drive: %+v", file.Parents[0].Id, file)
		f, err := sc.db.UpdateFile(nil, file)
		if err != nil {
			debug.Printf("failed to update levelDB for %v: %v", f.Id, err)
			// The write has happened to drive, but we failed to update the kernel.
			// The Changes API will update Fuse, and when the kernel metadata for
			// the parent directory expires, the new dir will become visible.
			req.RespondError(fuse.EIO)
			return
		}
		sc.db.FlushCachedInode(pInode)
		resp := &fuse.MkdirResponse{}
		resp.Node = fuse.NodeID(f.Inode)
		resp.EntryValid = *kernelRefresh
		resp.Attr = sc.attrFromNode(*f, inode)
		fuse.Debug(fmt.Sprintf("Mkdir(%v): %+v", req.Name, f))
		req.Respond(resp)
	*/
}

// Removes the inode described by req.Header.Node (doubles as rmdir)
// Nota bene: there is no check preventing the removal of a directory which
// contains files.
func (sc *serveConn) remove(req *fuse.RemoveRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO(asjoyner): shadeify
	// TODO: if allow_other, require uid == invoking uid to allow writes
	// TODO: consider disallowing deletion of directories with contents.. but what error?
	/*
		pInode := uint64(req.Header.Node)
		parent, err := sc.db.FileByInode(pInode)
		if err != nil {
			debug.Printf("failed to get parent file: %v", err)
			req.RespondError(fuse.EIO)
			return
		}
		for _, cInode := range parent.Children {
			child, err := sc.db.FileByInode(cInode)
			if err != nil {
				debug.Printf("failed to get child file: %v", err)
			}
			if child.Title == req.Name {
				sc.service.Files.Delete(child.Id).Do()
				sc.db.RemoveFileById(child.Id, nil)
				req.Respond()
				return
			}
		}
		req.RespondError(fuse.ENOENT)
	*/
}

// TODO(asjoyner): shadeify
// rename renames a file or directory, optionally reparenting it
func (sc *serveConn) rename(req *fuse.RenameRequest) {
	if *readOnly {
		fmt.Printf("attempt to rename while fs in readonly mode")
		req.RespondError(fuse.EPERM)
		return
	}
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

// TODO(asjoyner): shadeify
// Pass sequential writes on to the correct handle for uploading
func (sc *serveConn) write(req *fuse.WriteRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	/*
		h, err := sc.handleById(req.Handle)
		if err != nil {
			fuse.Debug(fmt.Sprintf("inodeByNodeID(%v): %v", req.Handle, err))
			req.RespondError(fuse.ESTALE)
			return
		}
		if h.lastByte != req.Offset {
			fuse.Debug(fmt.Sprintf("non-sequential write: got %v, expected %v", req.Offset, h.lastByte))
			req.RespondError(fuse.EIO)
			return
		}
		n, err := h.writer.Write(req.Data)
		if err != nil {
			req.RespondError(fuse.EIO)
			return
		}
		sc.Lock()
		h.lastByte += int64(n)
		sc.handles[req.Handle] = h
		sc.Unlock()
		req.Respond(&fuse.WriteResponse{n})
	*/
}
