package fusefs

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/memory"

	"bazil.org/fuse"
)

func init() {
	// This is helpful when the tests fail.
	//fstestutil.DebugByDefault()
	// Exercise chunk boundaries with smaller file sizes.
	DefaultChunkSizeBytes = 5 * 1024 * 1024
}

// TestFuseRead initializes a series of random chunks of data, calculates a
// shade.Sum and shade.File for each, placed in a series of subdirectories
// based on the Sum.  It uses PutChunk to put them into a memory client, and
// mounts a fusefs on that memory client.  It then uses filepath.Walk to
// iterate and validate the exposed filesystem.
func TestFuseRead(t *testing.T) {
	mountPoint, err := ioutil.TempDir("", "fusefsTest")
	if err != nil {
		t.Fatalf("could not acquire TempDir: %s", err)
	}
	defer tearDownDir(mountPoint)

	t.Logf("Mounting fuse filesystem at: %s", mountPoint)
	client, ffs, err := setupFuse(t, mountPoint)
	if err != nil {
		t.Fatalf("could not mount fuse: %s", err)
	}
	defer tearDownFuse(t, mountPoint)

	chunkSize := 8 * 256 // in bytes
	nc := 50             // number of chunks

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
		err := client.PutChunk([]byte(stringSum), chunk, nil)
		if err != nil {
			t.Fatalf("Failed to put chunk \"%x\": %s", stringSum, err)
		}
	}

	// Populate files, referencing those chunks, into the client
	// the directory scheme is to break apart the shasum into dirs:
	// deadbeef == de/ad/be/ff/deadbeef
	// .. unless it starts with 0, so we exercise files at / too.
	i := 0
	for chunkStringSum := range testChunks {
		filename := pathFromStringSum(chunkStringSum)
		file := shade.File{
			Filename: filename,
			Filesize: int64(chunkSize),
			Chunks: []shade.Chunk{{
				Index:  0,
				Sha256: []byte(chunkStringSum),
			}},
			Chunksize:     chunkSize,
			LastChunksize: chunkSize,
		}
		fj, err := file.ToJSON()
		if err != nil {
			t.Fatalf("test data is broken, could not marshal File: %s", err)
		}
		fileSum := sha256.Sum256(fj)
		if err := client.PutFile(fileSum[:], fj); err != nil {
			t.Errorf("failed to PutFile \"%x\": %s", fileSum[:], err)
		}
		i++
		t.Logf("Added to drive.Client %d: %s\n", i, filename)
	}

	// double check that the client is sane
	files, err := client.ListFiles()
	if err != nil {
		t.Fatal(err)
	}
	if nf := len(files); nf != nc {
		t.Fatalf("incomplete file set in client, want: %d, got: %d", nc, nf)
	}
	t.Logf("There are %d files known to the drive.Client.", nc)

	if err := ffs.Refresh(); err != nil {
		t.Fatalf("failed to refresh fuse fileserver: %s", err)
	}
	t.Logf("Drive client refreshed successfully.")

	seen := make(map[string]bool)
	visit := func(path string, f os.FileInfo, err error) error {
		if err1 := checkPath(t, testChunks, seen, mountPoint, path, f, err); err1 != nil {
			return err1
		}
		return nil
	}

	t.Logf("Attempting to walk the filesystem.")
	if err := filepath.Walk(mountPoint, visit); err != nil {
		t.Fatalf("filepath.Walk() returned %v", err)
	}

	if ns := len(seen); ns != nc {
		t.Errorf("incomplete file set in fuse, want: %d, got: %d", nc, ns)
		for chunk := range testChunks {
			if !seen[chunk] {
				t.Errorf("missing file: %x", chunk)
			}
		}
	}
}

// TestFuseRoundTrip creates an empty memory client, mounts a fusefs filesystem
// against it, generates random test data, writes it to the filesystem, reads
// it back out and validates it is as expected.
func TestFuseRoundtrip(t *testing.T) {
	mountPoint, err := ioutil.TempDir("", "fusefsTest")
	if err != nil {
		t.Fatalf("could not acquire TempDir: %s", err)
	}
	defer tearDownDir(mountPoint)

	fmt.Printf("Mounting fuse filesystem at: %s\n", mountPoint)
	if _, _, err := setupFuse(t, mountPoint); err != nil {
		t.Fatalf("could not mount fuse: %s", err)
	}
	defer tearDownFuse(t, mountPoint)

	maxFileSizeBytes := int64(DefaultChunkSizeBytes * 3)
	nf := 20 // number of files
	t.Logf("DefaultChunkSizeBytes: %d\n", DefaultChunkSizeBytes)
	t.Logf("maxFileSizeBytes: %d\n", maxFileSizeBytes)

	// Generate some random file contents
	t.Logf("Generating Random test data...\n")
	testFiles := make(map[string][]byte, nf)
	for i := 0; i < nf; i++ {
		fileSize, err := rand.Int(rand.Reader, big.NewInt(maxFileSizeBytes))
		if err != nil {
			t.Fatalf("could not calculate random filesize: %s", err)
		}
		n := make([]byte, int(fileSize.Int64()))
		rand.Read(n)
		s := sha256.Sum256(n)
		chunkSum := string(s[:])
		testFiles[chunkSum] = n
	}

	// Write those testFiles to the fusefs
	for stringSum, chunk := range testFiles {
		filename := path.Join(mountPoint, pathFromStringSum(stringSum))
		if err := os.MkdirAll(path.Dir(filename), 0700); err != nil {
			t.Fatalf(err.Error())
		}
		t.Logf("Writing %d bytes to %s\n", len(chunk), filename)
		start := time.Now()
		if err := ioutil.WriteFile(filename, chunk, 0400); err != nil {
			t.Fatalf(err.Error())
		}
		elapsed := time.Since(start)
		t.Logf("Took %s at %0.2fMB/s.\n", elapsed, float64(len(chunk))/1e6/elapsed.Seconds())
	}

	// Validate all the files have the right contents
	seen := make(map[string]bool)
	visit := func(path string, f os.FileInfo, err error) error {
		if err1 := checkPath(t, testFiles, seen, mountPoint, path, f, err); err1 != nil {
			return err1
		}
		return nil
	}

	t.Logf("Attempting to walk the filesystem.\n")
	if err := filepath.Walk(mountPoint, visit); err != nil {
		t.Fatalf("filepath.Walk() returned %v", err)
	}

	if ns := len(seen); ns != nf {
		t.Errorf("incomplete file set in fuse, want: %d, got: %d", nf, ns)
		for chunk := range testFiles {
			if !seen[chunk] {
				t.Errorf("missing file: %x", chunk)
			}
		}
	}

	// Delete all the test files, ensure they disappear.
	for stringSum := range testFiles {
		filename := path.Join(mountPoint, pathFromStringSum(stringSum))
		t.Logf("Removing %s\n", filename)
		if err := os.Remove(filename); err != nil {
			t.Errorf(err.Error())
		}
		if _, err := ioutil.ReadFile(filename); err == nil {
			t.Errorf("File still existed after delete: %s", filename)
		}
	}
}

func TestFuseDirs(t *testing.T) {
	mountPoint, err := ioutil.TempDir("", "fusefsTest")
	if err != nil {
		t.Fatalf("could not acquire TempDir: %s", err)
	}
	defer tearDownDir(mountPoint)

	fmt.Printf("Mounting fuse filesystem at: %s\n", mountPoint)
	if _, _, err := setupFuse(t, mountPoint); err != nil {
		t.Fatalf("could not mount fuse: %s", err)
	}
	defer tearDownFuse(t, mountPoint)

	testDir := path.Join(mountPoint, "hello")
	testbottom := path.Join(testDir, "world/a/b/c/d/e/f/g/h")
	if err := os.MkdirAll(testbottom, 0777); err != nil {
		t.Fatalf("could not create test directory: %s", err)
	}

	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("could not remove test directory: %s", err)
	}

	fh, err := os.Open(mountPoint)
	if err != nil {
		t.Fatalf("could not read open test directory: %s", err)
	}
	defer fh.Close()

	leftovers, err := fh.Readdir(0)
	if err != nil {
		t.Fatalf("could not read empty test directory: %s", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("mountPoint is not empty: %+v", leftovers)
	}
}

// setup returns the absolute path to a mountpoint for a fuse FS, and the
// memory-backed drive.Client which it is following.  The FS is configured not
// to refresh the drive.Client, you must call Refresh() if you update the
// client outside fuse.
func setupFuse(t *testing.T, mountPoint string) (drive.Client, *Server, error) {
	options := []fuse.MountOption{
		fuse.FSName("Shade"),
		fuse.NoAppleDouble(),
	}
	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		// often means the mountpoint is busy... but it's a TempDir...
		return nil, nil, fmt.Errorf("could not setup Fuse mount: %s", err)
	}

	mc, err := memory.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		return nil, nil, fmt.Errorf("NewClient() for test config failed: %s", err)
	}

	ffs, err := New(mc, conn, nil)
	if err != nil {
		t.Fatalf("fuse server initialization failed: %s", err)
	}
	go func() {
		err = ffs.Serve()
		if err != nil {
			t.Errorf("serving fuse connection failed: %s", err)
		}
	}()

	<-conn.Ready // returns when the FS is usable
	if conn.MountError != nil {
		return nil, nil, fmt.Errorf("fuse exited with an error: %s", err)
	}
	return mc, ffs, nil
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

func pathFromStringSum(sum string) string {
	chs := hex.EncodeToString([]byte(sum))
	var filename string
	if strings.HasPrefix(chs, "0") {
		filename = chs
	} else {
		// TODO: don't call Sprintf so often.  :)
		for i := 0; i <= len(chs)-8; i += 8 {
			filename = fmt.Sprintf("%s/%s", filename, chs[i:i+8])
		}
	}
	return strings.TrimPrefix(filename, "/")
}

func checkPath(t *testing.T, testChunks map[string][]byte, seen map[string]bool, mountPoint, path string, f os.FileInfo, err error) error {
	if err != nil {
		t.Errorf("failed to read path: %s", err)
		return err
	}
	if !f.IsDir() {
		t.Logf("Seen in FS %d: %s\n", len(seen)+1, path)
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			t.Error(err)
			return nil
		}
		filename := strings.TrimPrefix(path, mountPoint)
		filename = strings.Replace(filename, "/", "", -1)
		chs, err := hex.DecodeString(filename)
		if err != nil {
			t.Errorf("could not hex decode filename in Fuse FS: %s: %s", filename, err)
			return nil
		}
		if !bytes.Equal(contents, testChunks[string(chs)]) {
			t.Errorf("contents of %s did not match, want: %x, got: %x", filename, testChunks[string(chs)], contents)
			return nil
		}
		if seen[string(chs)] {
			t.Errorf("saw chunk twice: %x", chs)
		}
		seen[string(chs)] = true
	}
	return nil
}

// TestChunksForRead tests the function which calculates which chunks are
// necessary to service a fuse read request.  It initializes a set of chunks,
// then calls chunksForRead to ensure the correct series of chunks are returned
// for each offset.
func TestChunksForRead(t *testing.T) {
	f := &shade.File{
		Filename: "test",
		Filesize: 32,
		Chunks: []shade.Chunk{
			// chunks have artificial SHA sums to make identification easier
			{Index: 0, Sha256: []byte("0000")},
			{Index: 1, Sha256: []byte("1111")},
			{Index: 2, Sha256: []byte("2222")},
			{Index: 3, Sha256: []byte("3333")},
		},
		Chunksize: 8,
	}
	testSet := []struct {
		offset int64
		size   int64
		want   [][]byte
	}{
		{
			offset: 0,
			size:   1,
			want:   [][]byte{[]byte("0000")},
		},
		{
			offset: 0,
			size:   32,
			want: [][]byte{
				[]byte("0000"),
				[]byte("1111"),
				[]byte("2222"),
				[]byte("3333"),
			},
		},
		{
			offset: 31,
			size:   1,
			want:   [][]byte{[]byte("3333")},
		},
		{
			offset: 12,
			size:   8,
			want: [][]byte{
				[]byte("1111"),
				[]byte("2222"),
			},
		},
	}
	for _, ts := range testSet {
		cs, err := chunksForRead(f, ts.offset, ts.size)
		if err != nil {
			t.Errorf("unexpected error: chunksForRead(%+v, %d, %d): %s", f, ts.offset, ts.size, err)
			continue
		}
		if len(cs) != len(ts.want) {
			t.Errorf("chunksForRead(%+v, %d, %d), want: %s, got: %s", f, ts.offset, ts.size, ts.want, cs)
			continue
		}
		for i := 0; i < len(cs); i++ {
			if !bytes.Equal(cs[i], ts.want[i]) {
				t.Errorf("chunksForRead(%+v, %d, %d), want: %s, got: %s", f, ts.offset, ts.size, ts.want, cs)
			}
		}
	}
	_, err := chunksForRead(f, 33, 1)
	if err == nil {
		t.Errorf("expected error from chunksForRead(%+v, %d, %d), got nil", f, 33, 1)
	}
	_, err = chunksForRead(f, -1024, 1)
	if err == nil {
		t.Errorf("expected error from chunksForRead(%+v, %d, %d), got nil", f, -1024, 1)
	}
	_, err = chunksForRead(f, 0, -1)
	if err == nil {
		t.Errorf("expected error from chunksForRead(%+v, %d, %d), got nil", f, -1024, 1)
	}
}

// Test the method which updates a handle with new data during a write
func TestApplyWrite(t *testing.T) {
	// setup some initial data for the test
	mc, err := memory.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	posString := []byte("01234567")
	s := sha256.Sum256(posString)
	posStringSum := s[:]
	if err := mc.PutChunk(posStringSum, posString, nil); err != nil {
		t.Fatalf("Failed to put chunk \"%x\": %s", s, err)
	}

	testSet := []struct {
		before map[int64]shade.Chunk
		offset int64
		data   []byte
		after  map[int64][]byte
	}{
		{ // test 0
			map[int64]shade.Chunk{},
			0,
			[]byte("yep"),
			map[int64][]byte{0: []byte("yep")},
		},
		{ // test 1
			map[int64]shade.Chunk{
				0: {Index: 0, Sha256: posStringSum},
			},
			0,
			[]byte("yep"),
			map[int64][]byte{0: []byte("yep34567")},
		},
		{ // test 2, Example A
			map[int64]shade.Chunk{
				0: {Index: 0, Sha256: posStringSum},
			},
			0,
			[]byte("onechunk"),
			map[int64][]byte{0: []byte("onechunk")},
		},
		{ // test 3
			map[int64]shade.Chunk{
				0: {Index: 0, Sha256: posStringSum},
			},
			0,
			[]byte("just1more"),
			map[int64][]byte{0: []byte("just1mor"), 1: []byte("e")},
		},
		{ // test 4
			map[int64]shade.Chunk{
				0: {Index: 0, Sha256: posStringSum},
				1: {Index: 0, Sha256: posStringSum},
			},
			3,
			[]byte("straddle"),
			map[int64][]byte{0: []byte("012strad"), 1: []byte("dle34567")},
		},
		{ // test 5
			map[int64]shade.Chunk{
				0: {Index: 0, Sha256: posStringSum},
				1: {Index: 0, Sha256: posStringSum},
				2: {Index: 0, Sha256: posStringSum},
			},
			3,
			[]byte("straddlethreechunks"),
			map[int64][]byte{0: []byte("012strad"), 1: []byte("dlethree"), 2: []byte("chunks67")},
		},
	}
	for i, ts := range testSet {
		//fmt.Printf("test %d\n", i)
		h := handle{
			file: &shade.File{
				Chunksize: 8,
			},
			chunks: ts.before,
			dirty:  make(map[int64][]byte),
		}
		h.applyWrite(ts.data, ts.offset, mc)
		if len(h.dirty) != len(ts.after) {
			t.Fatalf("test %d: applyWrite(%s, %d..), wrong number of chunks, want: %d, got: %d", i, ts.data, ts.offset, len(ts.after), len(h.dirty))
		}
		for i := 0; i < len(h.dirty); i++ {
			i := int64(i)
			if !bytes.Equal(ts.after[i], h.dirty[i]) {
				t.Errorf("applyWrite(%q, %d..), chunk %d did not match.", ts.data, ts.offset, i)
				// This Errorf can be helpful, but is very verbose.
				t.Errorf("want: %q, got: %q", ts.after[i], h.dirty[i])
			}
		}
	}
}
