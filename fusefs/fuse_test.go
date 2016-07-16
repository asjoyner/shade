package fusefs

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/cache"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/memory"

	"bazil.org/fuse"
)

func TestFuseRead(t *testing.T) {
	mountPoint, err := ioutil.TempDir("", "fusefsTest")
	//fmt.Println("testing at: ", mountPoint)
	if err != nil {
		t.Fatalf("could not acquire TempDir: %s", err)
	}
	defer tearDownDir(mountPoint)

	client, err := setupFuse(t, mountPoint)
	if err != nil {
		t.Fatalf("could not mount fuse: %s", err)
	}
	defer tearDownFuse(t, mountPoint)

	chunkSize := 100 * 256 // in bytes
	nc := 4                // number of chunks

	// Generate some random file contents
	testChunks := make(map[string][]byte, nc)
	for i := 0; i < nc; i++ {
		n := make([]byte, chunkSize)
		rand.Read(n)
		s := sha256.Sum256(n)
		chunkSum := string(s[:])
		testChunks[chunkSum] = n
	}

	// Populate test chunks into the client
	for stringSum, chunk := range testChunks {
		err := client.PutChunk([]byte(stringSum), chunk)
		if err != nil {
			t.Fatalf("Failed to put chunk \"%x\": %s", stringSum, err)
		}
	}

	// Populate files, referencing those chunks, into the client
	// the directory scheme is to break apart the shasum into dirs:
	// deadbeef == de/ad/be/ff/deadbeef
	// .. unless it starts with 0, so we exercise files at / too.
	for chunkStringSum := range testChunks {
		chs := hex.EncodeToString([]byte(chunkStringSum))
		var filename string
		if strings.HasPrefix(chs, "0") {
			filename = chs
		} else {
			for i := 0; i <= len(chs)-8; i += 8 {
				filename = fmt.Sprintf("%s/%s", filename, chs[i:i+8])
			}
		}
		file := shade.File{
			Filename: filename,
			Filesize: int64(len(chunkStringSum)),
			Chunks: []shade.Chunk{{
				Index:  0,
				Sha256: []byte(chunkStringSum),
			}},
			Chunksize: chunkSize,
		}
		fj, err := file.ToJSON()
		if err != nil {
			t.Fatalf("test data is broken, could not marshal File: %s", err)
		}
		fileSum := sha256.Sum256(fj)
		if err := client.PutFile(fileSum[:], fj); err != nil {
			t.Errorf("failed to PutFile \"%x\": %s", fileSum[:], err)
		}
	}
	time.Sleep(1 * time.Second)

	visit := func(path string, f os.FileInfo, err error) error {
		// TODO(asjoyner): validate paths visited
		fmt.Printf("Visited: %s\n", path)
		return nil
	}

	if err := filepath.Walk(mountPoint, visit); err != nil {
		t.Fatalf("filepath.Walk() returned %v\n", err)
	}
}

// setup returns the absolute path to a mountpoint for a fuse FS, and the
// memory-backed drive.Client which it is following.  The Client is configured
// to refresh once per second, tests should sleep after PutFile / PutChunk.
// TODO(asjoyner): refresh the cache on demand
func setupFuse(t *testing.T, mountPoint string) (drive.Client, error) {
	options := []fuse.MountOption{
		fuse.FSName("Shade"),
		fuse.NoAppleDouble(),
	}
	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		// often means the mountpoint is busy... but it's a TempDir...
		return nil, fmt.Errorf("could not setup Fuse mount: %s", err)
	}

	mc, err := memory.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		return nil, fmt.Errorf("NewClient() for test config failed: %s", err)
	}

	refresh := time.NewTicker(1 * time.Second)
	r, err := cache.NewReader([]drive.Client{mc}, refresh)
	if err != nil {
		return nil, fmt.Errorf("could not initialize cache: %s", err)
	}

	ffs := New(r, conn)
	go func() {
		err = ffs.Serve()
		if err != nil {
			t.Errorf("fuse server initialization failed: %s", err)
		}
	}()

	<-conn.Ready // returns when the FS is usable
	if conn.MountError != nil {
		return nil, fmt.Errorf("fuse exited with an error: %s", err)
	}
	return mc, nil
}

func tearDownFuse(t *testing.T, dir string) {
	if err := fuse.Unmount(dir); err != nil {
		t.Fatalf("could not Unmount test dir: %s", err)
	}
}

func tearDownDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Could not clean up: %s", err)
	}
}
