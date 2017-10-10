// shadeutil contains tools for inspecting shade repositories.
package main

import (
	"flag"
	"os"

	"golang.org/x/net/context"

	"github.com/google/subcommands"

	// Subcommand imports
	_ "github.com/asjoyner/shade/cmd/shadeutil/ls"

	// Drive client provider imports
	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/cache"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/local"
	_ "github.com/asjoyner/shade/drive/memory"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
