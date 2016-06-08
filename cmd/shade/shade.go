// shade presents a fuse filesystem interface.
package main

import (
	"fmt"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/amazon"
	"github.com/asjoyner/shade/drive/google"
)

func main() {
	// TODO(asjoyner): setup fuse FS

	// read in the config
	configs, err := config.Read()
	if err != nil {
		fmt.Printf("could not parse config: %s", err)
	}

	// initialize drive client(s)
	var clients []drive.Client
	for _, conf := range configs {
		var c drive.Client
		var err error
		switch conf.Provider {
		case "amazon":
			c, err = amazon.NewClient(conf)
		case "google":
			c, err = google.NewClient(conf)
		}
		if err != nil {
			fmt.Printf("could not initialize %s drive client: %s", conf.Provider, err)
		}

		clients = append(clients, c)
	}

	// TODO(asjoyner): client.GetFiles()
	// TODO(asjoyner): update the Fuse FS

	f := shade.File{}
	f.Filename = "Hi"
	return
}
