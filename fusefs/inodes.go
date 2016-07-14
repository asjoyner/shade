package fusefs

import (
	"errors"
	"sync"
)

// inodeMap provides a mapping from fuse.Node to and from the Path that it
// corresponds to.
type inodeMap struct {
	inodes    map[uint64]string
	lastInode uint64
	sync.RWMutex
}

func NewInodeMap() inodeMap {
	return inodeMap{
		inodes: map[uint64]string{
			1: "/",
		},
		lastInode: 1,
	}
}

func (im *inodeMap) ToPath(inode uint64) (string, error) {
	im.RLock()
	defer im.RUnlock()
	if p, ok := im.inodes[inode]; ok {
		return p, nil
	}
	return "", errors.New("inode not allocated")
}

func (im *inodeMap) FromPath(p string) uint64 {
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

func (im *inodeMap) Release(inode uint64) {
	im.Lock()
	defer im.Unlock()
	delete(im.inodes, inode)
}
