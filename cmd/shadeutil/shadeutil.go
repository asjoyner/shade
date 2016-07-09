// shadeutil contains tools for inspecting shade repositories.
package main

import (
	"flag"
	"os"

	"golang.org/x/net/context"

	"github.com/google/subcommands"

	_ "github.com/asjoyner/shade/cmd/shadeutil/ls"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
