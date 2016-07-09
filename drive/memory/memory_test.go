package memory

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/asjoyner/shade/drive"
)

func TestFileRoundTrip(t *testing.T) {
	testFiles := map[string]string{
		"deadbeef": "kindaLikeJSON",
		"feedface": "almostLikeJSON",
		"4rbdfeed": "anApple?",
	}

	mc, _ := NewClient(drive.Config{Provider: "memory"})

	// Populate testFiles into the memory client
	for sum, file := range testFiles {
		err := mc.PutFile([]byte(sum), []byte(file))
		if err != nil {
			t.Fatalf("Failed to put test file: ", err)
		}
	}

	// Get all the files which were populated
	lfm, err := mc.ListFiles()
	if err != nil {
		t.Fatalf("Failed to retrieve file map: ", err)
	}
	for sum, file := range testFiles {
		var found bool
		for _, returnedSum := range lfm {
			if sum == string(returnedSum) {
				found = true
			}
		}
		if !found {
			t.Errorf("test file not returned: %s: %x", sum, file)
		}
	}
}

func TestChunkRoundTrip(t *testing.T) {
	mc, _ := NewClient(drive.Config{Provider: "memory"})

	// Generate some random test chunks
	testChunks := make([][]byte, 100)
	for i, _ := range testChunks {
		n := make([]byte, 100*256)
		rand.Read(n)
		testChunks[i] = n
	}

	// Populate test chunks into the memory client
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		err := mc.PutChunk(chunkSum[:], chunk)
		if err != nil {
			t.Fatalf("Failed to put test file \"%x\": ", chunkSum, err)
		}
	}

	// Get each chunk by its sha256sum
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		returnedChunk, err := mc.GetChunk(chunkSum[:])
		if err != nil {
			t.Fatalf("Failed to retrieve chunk \"%x\": %s", chunkSum, err)
		}
		if !bytes.Equal(chunk, returnedChunk) {
			t.Errorf("returned chunk does not match: %x", chunkSum)
		}
	}
}
