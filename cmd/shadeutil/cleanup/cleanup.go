package cleanup

import (
	"context"
	"flag"
	"fmt"

	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/umbrella"

	"github.com/google/subcommands"
)

func init() {
	subcommands.Register(&cleanupCmd{}, "")
}

type cleanupCmd struct {
	long bool
}

func (*cleanupCmd) Name() string     { return "cleanup" }
func (*cleanupCmd) Synopsis() string { return "Cleanup unused files and chunks." }
func (*cleanupCmd) Usage() string {
	return `cleanup
  Cleanup unused files and chunks.
`
}
func (*cleanupCmd) SetFlags(f *flag.FlagSet) { return }

func (p *cleanupCmd) Execute(_ context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	configPath := args[0].(*string)

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

	if err := umbrella.Cleanup(client); err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
