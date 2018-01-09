package drive

// This file contains test helper functions.  It is recommended to call all the
// public functions in this file in tests specific to each implementation of
// drive.Client.  This helps reduce duplication, and ensures they have
// uniform behavior.

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/asjoyner/shade"
)

const chunkSize uint64 = 100 * 256

// TestFileRoundTrip is a helper function, it allocates 100 random []byte,
// stores them in the provided client as files, retrieves them, and ensures all
// of the files were returned.
func TestFileRoundTrip(t *testing.T, c Client, numFiles uint64) {
	maxFiles := c.GetConfig().MaxFiles
	testFiles := randChunk(numFiles)

	// Populate testFiles into the client
	for stringSum, file := range testFiles {
		if err := c.PutFile([]byte(stringSum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file: ", err)
		}
	}

	// Populate them all again, which should not return an error.
	orderedFiles := make([]string, 0, numFiles)
	for stringSum, file := range testFiles {
		if err := c.PutFile([]byte(stringSum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file a second time: ", err)
		}
		// Make note of the order, for checking LRU behavior.
		orderedFiles = append(orderedFiles, stringSum)

		// The granularity of the local LRU is only 1 second, because it uses
		// mtime to track which files are most recent.  Sleep at the boundary of
		// the files we expect to be kept, so we ensure we know which ones will be
		// kept.
		if maxFiles > 0 && uint64(len(orderedFiles)) == numFiles-maxFiles {
			time.Sleep(1 * time.Second)
		}
	}
	retainedFiles := orderedFiles
	if maxFiles != 0 && maxFiles < numFiles {
		retainedFiles = orderedFiles[numFiles-maxFiles:]
	}

	// Check all the files are returned by ListFiles
	files, err := c.ListFiles()
	if err != nil {
		t.Fatalf("Failed to retrieve file map: %s", err)
	}
	if len(files) < len(retainedFiles) {
		t.Errorf("ListFiles returned too few files:")
		t.Errorf("want: %d, got: %d:", len(retainedFiles), len(files))
	}
	if maxFiles > 0 && uint64(len(files)) > maxFiles {
		t.Errorf("ListFiles returned too many files:")
		t.Errorf("want: %d, got: %d:", len(retainedFiles), len(files))
	}
	// make searching to see which files weren't returned faster
	returnedFiles := make(map[string]bool, len(files))
	for _, sum := range files {
		returnedFiles[string(sum)] = true
	}

	// check that the files returned are the ones expected
	for _, stringSum := range retainedFiles {
		if !returnedFiles[stringSum] {
			t.Errorf("test file not returned: %x", stringSum)
		}
	}

	// Check that the contents of the files can be retrived with GetFile
	for _, stringSum := range retainedFiles {
		returnedChunk, err := c.GetFile([]byte(stringSum))
		if err != nil {
			t.Errorf("Failed to retrieve chunk %x: %s", stringSum, err)
			continue
		}
		if !bytes.Equal(returnedChunk, testFiles[stringSum]) {
			t.Errorf("returned chunk for %x does not match", stringSum)
			//t.Errorf("got %x, want: %x", returnedChunk, testFiles[stringSum])
		}
	}
}

// TestChunkRoundTrip allocates 100 random []byte, stores them in the client as
// chunks, then retrieves each one by its Sum and compares the bytes that are
// returned.
func TestChunkRoundTrip(t *testing.T, c Client, numChunks uint64) {
	maxBytes := c.GetConfig().MaxChunkBytes
	testChunks := randChunk(uint64(numChunks))

	// Make a file out of the chunks
	file := shade.NewFile("testfile")
	i := 0
	for sum, _ := range testChunks {
		chunk := shade.NewChunk()
		chunk.Index = i
		chunk.Sha256 = []byte(sum)
		file.Chunks = append(file.Chunks, chunk)
		i++
	}
	file.LastChunksize = int(chunkSize)

	// Populate test chunks into the client
	for stringSum, chunk := range testChunks {
		err := c.PutChunk([]byte(stringSum), chunk, file)
		if err != nil {
			t.Fatalf("Failed to put chunk %x: %s", stringSum, err)
		} else {
			//t.Logf("Put chunk %x", stringSum)
		}
	}

	// Populate them all again, which should not return an error.
	var firstRetainedChunk uint64
	if maxBytes > 0 {
		firstRetainedChunk = maxBytes / chunkSize
	}
	orderedChunks := make([]string, 0, numChunks)
	for stringSum, chunk := range testChunks {
		err := c.PutChunk([]byte(stringSum), chunk, file)
		if err != nil {
			t.Fatalf("Failed to put test chunk a second time %x: %s", stringSum, err)
		}

		// Make note of the order, for checking MaxChunkBytes LRU behavior.
		orderedChunks = append(orderedChunks, stringSum)
		// The granularity of the local LRU is only 1 second, because it uses
		// mtime to track which chunks are most recent.  Sleep at the boundary
		// of the chunks we expect to be kept, so we ensure we know which
		// chunks will be kept.
		if maxBytes > 0 && uint64(len(orderedChunks)) == firstRetainedChunk {
			time.Sleep(1 * time.Second)
		}
	}

	// Get each chunk by its Sum
	for i, stringSum := range orderedChunks {
		returnedChunk, err := c.GetChunk([]byte(stringSum), file)
		// Check that the newest chunks were retained
		if uint64(i) >= firstRetainedChunk {
			if err != nil {
				t.Errorf("Failed to retrieve chunk %d of %d with sum %x: %s", i, len(orderedChunks), stringSum, err)
				continue
			}
			if !bytes.Equal(returnedChunk, testChunks[stringSum]) {
				t.Errorf("returned chunk for %xdoes not match", stringSum)
				// t.Errorf("got %q, want: %q", string(returnedChunk), string(chunk))
			}
		} else { // Check that the oldest chunks were removed
			_, err := c.GetChunk([]byte(stringSum), file)
			if err == nil {
				t.Errorf("Retrieved chunk %d of %d which should have been expired: %x", i, len(orderedChunks), stringSum)
			}
		}
	}
}

// TestParallelRoundTrip calls 10 copies of both test functions in parallel, to
// try to tickle race conditions in the implementation.
func TestParallelRoundTrip(t *testing.T, c Client, n uint64) {
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go runAndDone(TestFileRoundTrip, t, c, n, &wg)
		wg.Add(1)
		go runAndDone(TestChunkRoundTrip, t, c, n, &wg)
	}
	wg.Wait()
	return
}

func runAndDone(f func(*testing.T, Client, uint64), t *testing.T, c Client, n uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	f(t, c, n)
}

// Generate some random test chunks
func randChunk(n uint64) map[string][]byte {
	testChunks := make(map[string][]byte, n)
	for i := uint64(0); i < n; i++ {
		c := make([]byte, chunkSize)
		rand.Read(c)
		testChunks[string(shade.Sum(c))] = c
	}
	return testChunks
}
