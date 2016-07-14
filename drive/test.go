package drive

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestFileRoundTrip is a helper function, recommended from use in testing
// specific implementations of drive.Client.  This helps reduce duplication,
// and to ensure they have uniform behavior.
func TestFileRoundTrip(t *testing.T, c Client) {
	testFiles := map[string]string{
		"deadbeef": "kindaLikeJSON",
		"feedface": "almostLikeJSON",
		"4b1dfeed": "anApple?",
	}

	// Populate testFiles into the client
	for stringSum, file := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		if err := c.PutFile([]byte(sum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file: ", err)
		}
	}

	// Populate them all again, which should not return an error.
	for stringSum, file := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		if err := c.PutFile([]byte(sum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file a second time: ", err)
		}
	}

	// Get all the files which were populated
	files, err := c.ListFiles()
	if err != nil {
		t.Fatalf("Failed to retrieve file map: %s", err)
	}
	for stringSum := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		var found bool
		for _, returnedSum := range files {
			if bytes.Equal([]byte(sum), returnedSum) {
				found = true
			}
		}
		if !found {
			t.Errorf("test file not returned: %s", stringSum)
			t.Logf("%+v\n", files)
		}
	}
}

// TestChunkRoundTrip is a helper function, recommended from use in testing
// specific implementations of drive.Client.  This helps reduce duplication,
// and to ensure they have uniform behavior.
func TestChunkRoundTrip(t *testing.T, c Client) {
	// Generate some random test chunks
	testChunks := make([][]byte, 100)
	for i := range testChunks {
		n := make([]byte, 100*256)
		rand.Read(n)
		testChunks[i] = n
	}

	// Populate test chunks into the client
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		err := c.PutChunk(chunkSum[:], chunk)
		if err != nil {
			t.Fatalf("Failed to put chunk \"%x\": %s", chunkSum, err)
		}
	}

	// Populate them all again, which should not return an error.
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		err := c.PutChunk(chunkSum[:], chunk)
		if err != nil {
			t.Fatalf("Failed to put test chunk a second time \"%x\": %s", chunkSum, err)
		}
	}

	// Get each chunk by its sha256sum
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		returnedChunk, err := c.GetChunk(chunkSum[:])
		if err != nil {
			t.Errorf("Failed to retrieve chunk \"%x\": %s", chunkSum, err)
			continue
		}
		if !bytes.Equal(returnedChunk, chunk) {
			t.Errorf("returned chunk for \"%x\"does not match", chunkSum)
			// t.Errorf("got %q, want: %q", string(returnedChunk), string(chunk))
		}
	}
}
