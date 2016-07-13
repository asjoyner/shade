package drive

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFileRoundTrip(t *testing.T, c Client) {
	testFiles := map[string]string{
		"deadbeef": "kindaLikeJSON",
		"feedface": "almostLikeJSON",
		"4b1dfeed": "anApple?",
	}

	// Populate testFiles into the memory client
	for stringSum, file := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		if err := c.PutFile([]byte(sum), []byte(file)); err != nil {
			t.Fatalf("Failed to put test file: ", err)
		}
	}

	// Get all the files which were populated
	files, err := c.ListFiles()
	if err != nil {
		t.Errorf("Failed to retrieve file map: ", err)
		return
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
			//log.Printf("%+v\n", lfm)
		}
	}
}

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
			t.Fatalf("Failed to put test file \"%x\": ", chunkSum, err)
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
