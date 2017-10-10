package ls

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"text/tabwriter"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"

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
	config, err := config.Read(p.config)
	if err != nil {
		fmt.Printf("could not read config: %v", err)
		return subcommands.ExitFailure
	}

	// initialize client
	client, err := drive.NewClient(config)
	if err != nil {
		fmt.Printf("could not initialize client: %s\n", err)
		return subcommands.ExitFailure
	}

	file := &shade.File{}
	w := &tabwriter.Writer{}
	w.Init(os.Stdout, 0, 2, 1, ' ', 0)
	if p.long {
		fmt.Fprint(w, "\tid\t(sha)\tsize\tchunksize\tchunks\tmtime\tfilename\n")
	}
	fmt.Println(client.GetConfig().Provider)
	lfm, err := client.ListFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not get files: %v\n", err)
		return subcommands.ExitFailure
	}
	for id, sha256sum := range lfm {
		fileJSON, err := client.GetChunk(sha256sum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not get file %q: %v\n", id, err)
			continue
		}
		err = json.Unmarshal(fileJSON, file)
		if err != nil {
			fmt.Printf("failed to unmarshal: %v\n", err)
			continue
		}
		if p.long {
			fmt.Fprintf(w, "\t%v\t(%x)\t%v\t%v\t%v\t%v\t%v\n", id, sha256sum, file.Filesize, file.Chunksize, file.Chunks, file.ModifiedTime.Format(time.Stamp), file.Filename)
		} else {
			fmt.Fprintf(w, "\t%v\n", file.Filename)
		}
	}
	w.Flush()
	return subcommands.ExitSuccess
}
