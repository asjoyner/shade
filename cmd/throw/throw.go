// throw stores a file in the cloud, encrypted.
package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
)

// TODO(asjoyner): make chunksize a flag
var chunksize = flag.Int("chunksize", 16*1024*1024, "size of a chunk stored in Drive, in bytes")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nusage: %s [flags] <filename>\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	// read in the config
	clients, err := config.Clients()
	if err != nil {
		fmt.Printf("could not initialize clients: %s", err)
		os.Exit(1)
	}

	for _, c := range clients {
		fmt.Println(c)
	}

	// TODO(asjoyner): implement everything.

	f := shade.File{}
	f.Filename = "Hi"
	return
}
