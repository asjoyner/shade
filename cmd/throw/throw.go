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
	"sync"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"
	"github.com/golang/glog"
	"github.com/jpillora/backoff"

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
	numWorkers    = flag.Int("numUploaders", 20, "The number of goroutines to upload chunks in parallel.")
	maxRetries    = flag.Int("maxRetries", 10, "The number of times to try to write a chunk to persistent storage.")
)

type chunkToGo struct {
	chunk      shade.Chunk
	chunkbytes []byte
	manifest   *shade.File
}

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

	start := time.Now()

	// initialize client
	client, err := drive.NewClient(config)
	if err != nil {
		log.Fatalf("could not initialize client: %s\n", err)
	}

	// initialize the goroutines to upload chunks
	uploadRequests := make(chan chunkToGo)
	var workers sync.WaitGroup
	workers.Add(*numWorkers)
	for w := 1; w <= *numWorkers; w++ {
		go func(reqs chan chunkToGo) {
			for r := range reqs {
				numRetries := 0
				b := &backoff.Backoff{Factor: 4}
				for {
					numRetries++
					if err := client.PutChunk(r.chunk.Sha256, r.chunkbytes, r.manifest); err != nil {
						if numRetries >= *maxRetries {
							fmt.Fprintf(os.Stderr, "chunk upload failed: %s\n", err)
							os.Exit(1)
						}
						glog.Errorf("chunk write error, will retry: %s", err)
						time.Sleep(b.Duration())
						continue
					}
					b.Reset()
					break
				}
			}
			workers.Done()
			return
		}(uploadRequests)
	}

	filename := flag.Arg(0)

	manifest := shade.NewFile(flag.Arg(1))

	fh, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(3)
	}

	for {
		// Initialize chunk, to ensure each chunk uses a unique nonce
		chunk := shade.NewChunk()
		chunk.Index = len(manifest.Chunks)
		// Initialize chunkbytes, so it's safe for concurrent access later
		chunkbytes := make([]byte, manifest.Chunksize)

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
		uploadRequests <- chunkToGo{chunk, chunkbytes, manifest}
	}
	close(uploadRequests)
	workers.Wait()

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

	elapsed := time.Since(start)
	size := manifest.Filesize / 1024 / 1024
	MBps := size / elapsed.Nanoseconds() / 1000000
	fmt.Printf("Uploaded %d MB in %s at %dMB/s.\n", size, elapsed, MBps)
}
