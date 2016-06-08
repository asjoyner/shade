// shade presents a fuse filesystem interface.
package main

import (
	"fmt"
	"os"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
)

func main() {
	// TODO(asjoyner): setup fuse FS

	// read in the config
	clients, err := config.Clients()
	if err != nil {
		fmt.Printf("could not initialize clients: %s", err)
		os.Exit(1)
	}

	// TODO(asjoyner): client.GetFiles()
	fmt.Printf("%q", clients)

	// TODO(asjoyner): update the Fuse FS
	f := shade.File{}
	f.Filename = "Hi"
	return
}
