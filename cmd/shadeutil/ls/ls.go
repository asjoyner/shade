package ls

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"

	"golang.org/x/net/context"

	"github.com/google/subcommands"
)

func init() {
	subcommands.Register(&lsCmd{}, "")
}

var (
	defaultConfig = path.Join(shade.ConfigDir(), "config.json")
)

type lsCmd struct {
	long   bool
	config string
}

func (*lsCmd) Name() string     { return "ls" }
func (*lsCmd) Synopsis() string { return "List files in the respository." }
func (*lsCmd) Usage() string {
	return `ls [-l] [-f FILE]:
  List all the files in the configured shade repositories.
`
}

func (p *lsCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&p.long, "l", false, "Long format listing")
	f.StringVar(&p.config, "f", defaultConfig, "Path to shade config")
}

func (p *lsCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// read in the config
	clients, err := config.Clients(p.config)
	if err != nil {
		fmt.Printf("could not initialize clients: %s", err)
		return subcommands.ExitFailure
	}

	file := &shade.File{}
	for _, client := range clients {
		fmt.Println(client.GetConfig().Provider)
		lfm, err := client.ListFiles()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not get files: %s\n", err)
			return subcommands.ExitFailure
		}
		for id, sha256sum := range lfm {
			fileJSON, err := client.GetChunk(sha256sum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not get file %q: %s\n", id, err)
				continue
			}
			err = json.Unmarshal(fileJSON, file)
			if err != nil {
				fmt.Printf("failed to unmarshal: %s\n", err)
				continue
			}
			if p.long {
				fmt.Printf("  %s (%x):\n    %s\n", id, sha256sum, file)
			} else {
				fmt.Printf("  %s (%x)\n", id, sha256sum)
			}
		}
	}
	return subcommands.ExitSuccess
}
