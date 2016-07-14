package fusefs

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/asjoyner/shade/cache"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/memory"

	"bazil.org/fuse"
)

func TestFuseRead(t *testing.T) {
	mountPoint, err := ioutil.TempDir("", "fusefsTest")
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown(mountPoint)

	options := []fuse.MountOption{
		fuse.FSName("Shade"),
		fuse.AllowOther(),
		fuse.NoAppleDouble(),
	}
	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		// often means the mountpoint is busy... but it's a TempDir...
		t.Fatalf("Could not setup Fuse mount: %s", err)
	}

	mc, err := memory.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	refresh := time.NewTicker(1 * time.Second)
	r, err := cache.NewReader([]drive.Client{mc}, refresh)
	if err != nil {
		t.Fatalf("could not initialize cache: %s", err)
	}

	ffs := New(r, conn)
	go func() {
		err = ffs.Serve()
		if err != nil {
			t.Fatalf("fuse server initialization failed: %s", err)
		}
	}()

	<-conn.Ready // returns when the FS is usable
	if conn.MountError != nil {
		t.Fatalf("fuse exited with an error: %s", err)
	}

	if err := fuse.Unmount(mountPoint); err != nil {
		t.Fatalf("could not Unmount test dir: %s", err)
	}

}

func tearDown(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Could not clean up: %s", err)
	}
}
