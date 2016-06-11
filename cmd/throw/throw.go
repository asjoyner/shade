// throw stores a file in the cloud, encrypted.
package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
)

// TODO(asjoyner): make chunksize a flag
var chunksize = flag.Int("chunksize", 16*1024*1024, "size of a chunk stored in Drive, in bytes")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nusage: %s [flags] <filename> <destination filename>\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}

	// read in the config
	clients, err := config.Clients()
	if err != nil {
		fmt.Printf("could not initialize clients: %s", err)
		os.Exit(1)
	}

	filename := flag.Arg(1)
	// TODO(asjoyner): stat file

	manifest := shade.File{
		Filename:  flag.Arg(2),
		Chunksize: *chunksize,
	}
	// TODO(asjoyner): generate AES key

	fh, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(3)
	}

	chunk := shade.Chunk{}
	chunkbytes := make([]byte, *chunksize)
	for {
		// Read a chunk
		len, err := fh.Read(chunkbytes)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(3)
		}
		manifest.Filesize += int64(len)
		// TODO(asjoyner): optionally, encrypt bytes
		a := sha256.Sum256(chunkbytes)
		chunk.Sha256 = a[:]

		// upload the chunk
		for _, c := range clients {
			c.PutChunk(chunk.Sha256, chunkbytes)
		}

		manifest.Chunks = append(manifest.Chunks, chunk)
		chunk.Index += 1
	}

	// TODO(asjoyner): optionally, encrypt manifest
	// upload the manifest
	for _, c := range clients {
		c.PutFile("")
	}

	// TODO(asjoyner): encrypt manifest

	f := shade.File{}
	f.Filename = "Hi"
	return
}
