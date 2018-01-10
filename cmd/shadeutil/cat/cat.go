package cat

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"

	"github.com/google/subcommands"
)

func init() {
	subcommands.Register(&catCmd{}, "")
}

type catCmd struct {
	long bool
}

func (*catCmd) Name() string     { return "cat" }
func (*catCmd) Synopsis() string { return "List files in the respository." }
func (*catCmd) Usage() string {
	return `cat <FILE>:
  Print the named file to STDOUT.
`
}
func (*catCmd) SetFlags(f *flag.FlagSet) { return }

func (p *catCmd) Execute(_ context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	// parse the filename
	configPath := args[0].(*string)
	if f.NArg() != 1 {
		fmt.Printf("unexpected number of arguments to cat; want: 1, got: %d\n", f.NArg())
		return subcommands.ExitFailure
	}
	filename := f.Arg(0)

	// read in the config
	config, err := config.Read(*configPath)
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

	lfm, err := client.ListFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not get files: %v\n", err)
		return subcommands.ExitFailure
	}
	file := &shade.File{}
	for _, sha256sum := range lfm {
		fileJSON, err := client.GetFile(sha256sum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not get file %q: %v\n", sha256sum, err)
			continue
		}
		err = json.Unmarshal(fileJSON, file)
		if err != nil {
			fmt.Printf("failed to unmarshal: %v\n", err)
			continue
		}
		if file.Filename != filename {
			continue
		}
		for _, chunk := range file.Chunks {
			c, err := client.GetChunk(chunk.Sha256, file)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not get chunk %v of file: %v\n", chunk.Sha256, err)
				return subcommands.ExitFailure
			}
			if _, err := os.Stdout.Write(c); err != nil {
				fmt.Fprintf(os.Stderr, "could not write to stdout: %v\n", err)
				return subcommands.ExitFailure
			}
		}
		return subcommands.ExitSuccess
	}
	fmt.Fprintf(os.Stderr, "no such file: %v\n", filename)
	return subcommands.ExitFailure
}
