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

	"github.com/asjoyner/shade"
)

// TestFileRoundTrip is a helper function,
func TestFileRoundTrip(t *testing.T, c Client) {
	testFiles := randChunk(100)

	// Populate testFiles into the client
	for stringSum, file := range testFiles {
		if err := c.PutFile([]byte(stringSum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file: ", err)
		}
	}

	// Populate them all again, which should not return an error.
	for stringSum, file := range testFiles {
		if err := c.PutFile([]byte(stringSum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file a second time: ", err)
		}
	}

	// Get all the files which were populated
	files, err := c.ListFiles()
	if err != nil {
		t.Fatalf("Failed to retrieve file map: %s", err)
	}
	for stringSum := range testFiles {
		var found bool
		for _, returnedSum := range files {
			if bytes.Equal([]byte(stringSum), returnedSum) {
				found = true
			}
		}
		if !found {
			t.Errorf("test file not returned: %x", stringSum)
			t.Logf("%+v\n", files)
		}
	}
}

// TestChunkRoundTrip allocates 100 random chunks of data, stores them in the
// client, then retrieves each one by the its Sum and compares the bytes that
// are returned.
func TestChunkRoundTrip(t *testing.T, c Client) {
	testChunks := randChunk(100)

	// Populate test chunks into the client
	for stringSum, chunk := range testChunks {
		err := c.PutChunk([]byte(stringSum), chunk)
		if err != nil {
			t.Fatalf("Failed to put chunk \"%x\": %s", stringSum, err)
		}
	}

	// Populate them all again, which should not return an error.
	for stringSum, chunk := range testChunks {
		err := c.PutChunk([]byte(stringSum), chunk)
		if err != nil {
			t.Fatalf("Failed to put test chunk a second time \"%x\": %s", stringSum, err)
		}
	}

	// Get each chunk by its Sum
	for stringSum, chunk := range testChunks {
		returnedChunk, err := c.GetChunk([]byte(stringSum))
		if err != nil {
			t.Errorf("Failed to retrieve chunk \"%x\": %s", stringSum, err)
			continue
		}
		if !bytes.Equal(returnedChunk, chunk) {
			t.Errorf("returned chunk for \"%x\"does not match", stringSum)
			// t.Errorf("got %q, want: %q", string(returnedChunk), string(chunk))
		}
	}
}

// TestParallelRoundTrip calls 10 copies of both test functions in parallel, to
// try to tickle race conditions in the implementation.
func TestParallelRoundTrip(t *testing.T, c Client) {
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go runAndDone(TestFileRoundTrip, t, c, &wg)
		wg.Add(1)
		go runAndDone(TestChunkRoundTrip, t, c, &wg)
	}
	wg.Wait()
	return
}

func runAndDone(f func(*testing.T, Client), t *testing.T, c Client, wg *sync.WaitGroup) {
	defer wg.Done()
	f(t, c)
}

// Generate some random test chunks
func randChunk(n int) map[string][]byte {
	testChunks := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		c := make([]byte, 100*256)
		rand.Read(c)
		testChunks[string(shade.Sum(c))] = c
	}
	return testChunks
}
