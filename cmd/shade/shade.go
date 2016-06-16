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

	files := make(map[string]*shade.File)
	for _, c := range clients {
		newFiles, err := c.GetFiles()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not get files: %s\n", err)
			os.Exit(1)
		}
		for _, fileJSON := range newFiles {
			file := &shade.File{}
			err := json.Unmarshal(fileJSON, file)
			if err != nil {
				fmt.Printf("failed to unmarshal: %s\n", err)
				continue
			}
			// append-only, for now... deletes to come later.
			files[file.Filename] = file
		}
	}
	for id, f := range files {
		fmt.Printf("%s: %s\n", id, f)
	}

	return
}
