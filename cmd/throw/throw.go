// throw stores a file in the cloud, encrypted.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"

	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/cache"
	_ "github.com/asjoyner/shade/drive/encrypt"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/local"
	_ "github.com/asjoyner/shade/drive/memory"
)

var (
	defaultConfig = path.Join(shade.ConfigDir(), "config.json")
	configPath    = flag.String("config", defaultConfig, "shade config file")
)

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
	config, err := config.Read(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not initialize clients: %s\n", err)
		os.Exit(1)
	}

	// initialize client
	client, err := drive.NewClient(config)
	if err != nil {
		log.Fatalf("could not initialize client: %s\n", err)
	}

	filename := flag.Arg(0)

	manifest := shade.NewFile(flag.Arg(1))

	fh, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(3)
	}

	chunk := shade.NewChunk()
	chunkbytes := make([]byte, manifest.Chunksize)
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
		if len < manifest.Chunksize {
			chunkbytes = chunkbytes[:len]
			manifest.LastChunksize = len
		}

		a := sha256.Sum256(chunkbytes)
		chunk.Sha256 = a[:]

		manifest.Chunks = append(manifest.Chunks, chunk)

		// upload the chunk
		if err := client.PutChunk(chunk.Sha256, chunkbytes, manifest); err != nil {
			fmt.Fprintf(os.Stderr, "chunk upload failed: %s\n", err)
			os.Exit(1)
		}

		chunk.Index++
	}

	jm, err := json.Marshal(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not marshal file manifest: %s\n", err)
		os.Exit(1)
	}
	// upload the manifest
	a := sha256.Sum256(jm)
	if err := client.PutFile(a[:], jm); err != nil {
		fmt.Fprintf(os.Stderr, "manifest upload failed: %s\n", err)
		os.Exit(1)
	}
}
