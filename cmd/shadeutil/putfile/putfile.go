package putfile

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"

	"github.com/google/subcommands"
)

func init() {
	subcommands.Register(&putfileCmd{}, "")
}

type putfileCmd struct {
	long bool
}

func (*putfileCmd) Name() string     { return "putfile" }
func (*putfileCmd) Synopsis() string { return "List files in the respository." }
func (*putfileCmd) Usage() string {
	return `putfile <path>:
  Call client.PutFile() on the contents of the file at <path>
`
}
func (*putfileCmd) SetFlags(f *flag.FlagSet) { return }

func (p *putfileCmd) Execute(_ context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	// parse the filename
	configPath := args[0].(*string)
	if f.NArg() != 1 {
		fmt.Printf("unexpected number of arguments to putfile; want: 1, got: %d\n", f.NArg())
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

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		return subcommands.ExitFailure
	}
	if err := client.PutFile(shade.Sum(data), data); err != nil {
		fmt.Fprintf(os.Stderr, "PutChunk: %v\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
