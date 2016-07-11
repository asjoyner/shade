package localdrive

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/asjoyner/shade/drive"
)

var config drive.Config

func TestFileRoundTrip(t *testing.T) {
	testFiles := map[string]string{
		"deadbeef": "kindaLikeJSON",
		"feedface": "almostLikeJSON",
		"4b1dfeed": "anApple?",
	}

	ld, _ := NewClient(config)

	// Populate testFiles into the client
	for stringSum, file := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		if err := ld.PutFile([]byte(sum), []byte(file)); err != nil {
			t.Fatalf("Failed to put test file: ", err)
		}
	}

	// Get all the files which were populated
	lfm, err := ld.ListFiles()
	if err != nil {
		t.Fatalf("Failed to retrieve file map: ", err)
	}
	for stringSum, file := range testFiles {
		sum, err := hex.DecodeString(stringSum)
		if err != nil {
			t.Fatalf("testFile %s is broken: %s", stringSum, err)
		}
		var found bool
		for _, returnedSum := range lfm {
			if bytes.Equal([]byte(sum), returnedSum) {
				found = true
			}
		}
		if !found {
			fmt.Printf("%+v\n", lfm)
			t.Errorf("test file not returned: %s: %s", stringSum, file)
		}
	}
}

func TestChunkRoundTrip(t *testing.T) {
	ld, _ := NewClient(config)

	// Generate some random test chunks
	testChunks := make([][]byte, 100)
	for i, _ := range testChunks {
		n := make([]byte, 100*256)
		rand.Read(n)
		testChunks[i] = n
	}

	// Populate test chunks into the client
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		err := ld.PutChunk(chunkSum[:], chunk)
		if err != nil {
			t.Fatalf("Failed to put test file \"%x\": ", chunkSum, err)
		}
	}

	// Get each chunk by its sha256sum
	for _, chunk := range testChunks {
		chunkSum := sha256.Sum256(chunk)
		returnedChunk, err := ld.GetChunk(chunkSum[:])
		if err != nil {
			t.Fatalf("Failed to retrieve chunk \"%x\": %s", chunkSum, err)
		}
		if !bytes.Equal(chunk, returnedChunk) {
			t.Errorf("returned chunk does not match: %x", chunkSum)
		}
	}
}

func TestMain(m *testing.M) {
	dir, err := ioutil.TempDir("", "localdiskTest")
	if err != nil {
		log.Fatal(err)
	}
	fp := path.Join(dir, "files")
	os.Mkdir(fp, 0700)
	cp := path.Join(dir, "chunks")
	os.Mkdir(cp, 0700)

	config = drive.Config{
		Provider:      "localdisk",
		FileParentID:  fp,
		ChunkParentID: cp,
	}
	r := m.Run()
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Could not clean up: %s", err)
	}
	os.Exit(r)
}
