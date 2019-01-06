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
	"runtime"
	"runtime/pprof"
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
	_ "github.com/asjoyner/shade/drive/win"
)

var (
	defaultConfig = path.Join(shade.ConfigDir(), "config.json")
	configPath    = flag.String("config", defaultConfig, "shade config file")
	// numUploaders has diminishing returns after about 3-4, and meaningfully
	// increases memory usage.  Setting it too high will almost certainly cause
	// OOMs when upload files of more than trivial size.
	numUploaders = flag.Int("numUploaders", 3, "The number of goroutines to upload chunks in parallel.")
	maxRetries   = flag.Int("maxRetries", 10, "The number of times to try to write a chunk to persistent storage.")
	// maxChunks serves as a backstop against accidentaly uploading /dev/urandom
	// ad infinitum, and can be set to a lower value to facilitate testing memory
	// usage, etc.  You may find testdata/config.win.json helpful for this.
	maxChunks = flag.Int("maxChunks", 1000000, "The maximum number of chunks to read for a given file.")
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
		glog.Flush()
		os.Exit(2)
	}

	// read in the config
	config, err := config.Read(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not initialize clients: %s\n", err)
		glog.Flush()
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
	workers.Add(*numUploaders)
	for w := 1; w <= *numUploaders; w++ {
		go func(reqs chan chunkToGo) {
			for r := range reqs {
				numRetries := 0
				b := &backoff.Backoff{Factor: 4}
				for {
					numRetries++
					if err := client.PutChunk(r.chunk.Sha256, r.chunkbytes, r.manifest); err != nil {
						if numRetries >= *maxRetries {
							fmt.Fprintf(os.Stderr, "chunk upload failed: %s\n", err)
							glog.Flush()
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
		glog.Flush()
		os.Exit(3)
	}

	var rt runtime.MemStats
	for {
		// Initialize chunk, to ensure each chunk uses a unique nonce
		chunk := shade.NewChunk()
		chunk.Index = len(manifest.Chunks)
		// Initialize chunkbytes, so it's safe for concurrent access later
		chunkbytes := make([]byte, manifest.Chunksize)

		// Read a chunk
		numBytes, err := fh.Read(chunkbytes)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			glog.Flush()
			os.Exit(3)
		} else if len(manifest.Chunks) >= *maxChunks {
			glog.Info("Reached the maximum number of chunks in a single file.")
			break
		}
		manifest.Filesize += int64(numBytes)

		// truncate the chunkbytes array on the last read
		if numBytes < manifest.Chunksize {
			chunkbytes = chunkbytes[:numBytes]
			manifest.LastChunksize = numBytes
		}

		a := sha256.Sum256(chunkbytes)
		chunk.Sha256 = a[:]

		manifest.Chunks = append(manifest.Chunks, chunk)

		if glog.V(3) {
			if (len(manifest.Chunks) % 10) == 0 {
				runtime.ReadMemStats(&rt)
				glog.Infof("%d chunks: %0.2f MBytes Heap, %0.2f MBytes Sys\n", len(manifest.Chunks), float64(rt.Alloc)/1024/1024, float64(rt.Sys)/1024/1024)

				if glog.V(9) {
					f, err := os.Create("/tmp/throw.mprof")
					if err != nil {
						log.Fatal(err)
					}
					pprof.WriteHeapProfile(f)
					f.Close()
				}
			}
		}
		// upload the chunk
		uploadRequests <- chunkToGo{chunk, chunkbytes, manifest}
	}
	close(uploadRequests)
	workers.Wait()

	jm, err := json.Marshal(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not marshal file manifest: %s\n", err)
		glog.Flush()
		os.Exit(1)
	}
	// upload the manifest
	a := sha256.Sum256(jm)
	if err := client.PutFile(a[:], jm); err != nil {
		fmt.Fprintf(os.Stderr, "manifest upload failed: %s\n", err)
		glog.Flush()
		os.Exit(1)
	}

	elapsed := time.Since(start)
	size := manifest.Filesize / 1024 / 1024
	MBps := float64(size) / (float64(elapsed.Nanoseconds()) / 1000000000)
	fmt.Printf("Uploaded %d MB in %s at %0.2f MB/s.\n", size, elapsed, MBps)
	glog.Flush()
}
