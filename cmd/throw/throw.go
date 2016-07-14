// throw stores a file in the cloud, encrypted.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"

	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/local"
	_ "github.com/asjoyner/shade/drive/memory"
)

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
		fmt.Fprintf(os.Stderr, "could not initialize clients: %s\n", err)
		os.Exit(1)
	}

	filename := flag.Arg(0)

	manifest := shade.File{
		Filename:  flag.Arg(1),
		Chunksize: *chunksize,
	}
	// TODO(asjoyner): generate AES key

	fh, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
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
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(3)
		}
		manifest.Filesize += int64(len)

		// truncate the chunkbytes array on the last read
		if len < *chunksize {
			chunkbytes = chunkbytes[:len]
		}

		// TODO(asjoyner): optionally, encrypt bytes
		a := sha256.Sum256(chunkbytes)
		chunk.Sha256 = a[:]

		// upload the chunk
		for _, c := range clients {
			err := c.PutChunk(chunk.Sha256, chunkbytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "chunk upload failed: %s\n", err)
				os.Exit(1)
			}
		}

		manifest.Chunks = append(manifest.Chunks, chunk)
		chunk.Index++
	}

	jm, err := json.Marshal(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not marshal file manifest: %s\n", err)
		os.Exit(1)
	}
	// TODO(asjoyner): optionally, encrypt the manifest
	// upload the manifest
	for _, c := range clients {
		a := sha256.Sum256(jm)
		err := c.PutFile(a[:], jm)
		if err != nil {
			fmt.Fprintf(os.Stderr, "manifest upload failed: %s\n", err)
			os.Exit(1)
		}
	}
}
