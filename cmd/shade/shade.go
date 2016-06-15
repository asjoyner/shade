// shade presents a fuse filesystem interface.
package main

import (
	"encoding/json"
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

	files := make(map[string]shade.File)
	for _, c := range clients {
		newFiles, err := c.GetFiles()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not get files: %s\n", err)
			os.Exit(1)
		}
		for _, fj := range newFiles {
			var file shade.File
			err := json.Unmarshal(fj, file)
			if err != nil {
				fmt.Printf("failed to unmarshal: %s", err)
				continue
			}
			// append-only, for now... deletes to come later.
			files[file.Filename] = file
		}
	}
	for _, f := range files {
		fmt.Println(f)
	}

	// TODO(asjoyner): update the Fuse FS
	f := shade.File{}
	f.Filename = "Hi"
	return
}
