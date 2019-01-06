// shadeutil contains tools for inspecting shade repositories.
package main

import (
	"flag"
	"os"
	"path"

	"golang.org/x/net/context"

	"github.com/golang/glog"
	"github.com/google/subcommands"

	// Subcommand imports
	"github.com/asjoyner/shade"
	_ "github.com/asjoyner/shade/cmd/shadeutil/cat"
	_ "github.com/asjoyner/shade/cmd/shadeutil/cleanup"
	_ "github.com/asjoyner/shade/cmd/shadeutil/genkeys"
	_ "github.com/asjoyner/shade/cmd/shadeutil/ls"

	// Drive client provider imports
	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/cache"
	_ "github.com/asjoyner/shade/drive/encrypt"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/local"
	_ "github.com/asjoyner/shade/drive/memory"
)

var (
	defaultConfig = path.Join(shade.ConfigDir(), "config.json")
)

func main() {
	configPath := flag.String("config", defaultConfig, "Path to shade config")
	subcommands.ImportantFlag("config")
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	flag.Parse()

	ctx := context.Background()
	exitValue := subcommands.Execute(ctx, configPath)
	glog.Flush()
	os.Exit(int(exitValue))
}
