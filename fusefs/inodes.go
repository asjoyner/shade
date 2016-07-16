package fusefs

import (
	"errors"
	"sync"
)

// InodeMap provides a mapping from fuse.Node to and from the Path that it
// corresponds to.
type InodeMap struct {
	sync.RWMutex // protects acess to all fields of the struct
	inodes       map[uint64]string
	lastInode    uint64
}

// NewInodeMap returns an initialized InodeMap. Initially, it knows of only the
// path to the root inode.
func NewInodeMap() *InodeMap {
	return &InodeMap{
		inodes: map[uint64]string{
			1: "/",
		},
		lastInode: 1,
	}
}

// FromPath returns the inode allocated for a given path.  If no inode has been
// allocated for that path yet, it allocates a new one and returns it.
func (im *InodeMap) FromPath(p string) uint64 {
	im.RLock()
	for inode, path := range im.inodes {
		if p == path {
			return inode
		}
	}
	im.RUnlock()

	// allocate the inode
	im.Lock()
	defer im.Unlock()
	im.lastInode++
	im.inodes[im.lastInode] = p
	return im.lastInode
}

// ToPath returns the path which was allocated to inode.  If inode has not yet
// been allocated, ToPath returns an error.
func (im *InodeMap) ToPath(inode uint64) (string, error) {
	im.RLock()
	defer im.RUnlock()
	if p, ok := im.inodes[inode]; ok {
		return p, nil
	}
	return "", errors.New("inode not allocated")
}

// Release deletes the mapping from an inode to a given path.
func (im *InodeMap) Release(inode uint64) {
	im.Lock()
	defer im.Unlock()
	delete(im.inodes, inode)
}
