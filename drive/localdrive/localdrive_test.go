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

var config drive.Config

func TestFileRoundTrip(t *testing.T) {
	ld, _ := NewClient(config)
	drive.TestFileRoundTrip(t, ld)
}

func TestChunkRoundTrip(t *testing.T) {
	ld, _ := NewClient(config)
	drive.TestChunkRoundTrip(t, ld)
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
