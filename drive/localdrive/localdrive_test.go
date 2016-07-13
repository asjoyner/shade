package localdrive

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/asjoyner/shade/drive"
)

func TestFileRoundTrip(t *testing.T) {
	dir, err := ioutil.TempDir("", "localdiskTest")
	if err != nil {
		log.Fatal(err)
	}
	defer tearDown(dir)
	ld, err := NewClient(drive.Config{
		Provider:      "localdisk",
		FileParentID:  path.Join(dir, "files"),
		ChunkParentID: path.Join(dir, "chunks"),
	})
	if err != nil {
		t.Fatalf("initializing client: %s", err)
	}
	drive.TestFileRoundTrip(t, ld)
}

func TestChunkRoundTrip(t *testing.T) {
	dir, err := ioutil.TempDir("", "localdiskTest")
	if err != nil {
		log.Fatal(err)
	}
	defer tearDown(dir)
	ld, err := NewClient(drive.Config{
		Provider:      "localdisk",
		FileParentID:  path.Join(dir, "files"),
		ChunkParentID: path.Join(dir, "chunks"),
	})
	if err != nil {
		t.Fatalf("initializing client: %s", err)
	}
	drive.TestChunkRoundTrip(t, ld)
}

func TestDirRequired(t *testing.T) {
	_, err := NewClient(drive.Config{
		Provider:      "localdisk",
		FileParentID:  "/proc/sure/hope/this/fails",
		ChunkParentID: "/proc/sure/hope/this/fails",
	})
	if err == nil {
		t.Fatalf("expected error on inaccessible directory: %s", err)
	}
}

func tearDown(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Could not clean up: %s", err)
	}
}
