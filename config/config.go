package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"

	"github.com/asjoyner/shade/drive"
)

// Read finds, reads, parses, and returns the config.
func Read() ([]drive.Config, error) {
	filename := configPath()
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("ReadFile(%q): %s", filename, err)
	}

	configs, err := parseConfig(contents)
	if err != nil {
		return nil, fmt.Errorf("parsing %q: %s", filename, err)
	}

	return configs, nil
}

// parseConfig is broken out primarily to test unmarshaling of various example
// configuration objects.
func parseConfig(contents []byte) ([]drive.Config, error) {
	var configs []drive.Config
	if err := json.Unmarshal(contents, &configs); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %s", err)
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("no provider in config file")
	}
	return configs, nil
}

// configPath identifies the correct path to the config on various operating
// systems.
func configPath() string {
	dir := "."
	switch runtime.GOOS {
	case "darwin":
		dir = path.Join(os.Getenv("HOME"), "Library", "Application Support", "shade")
	case "linux", "freebsd":
		dir = path.Join(os.Getenv("HOME"), ".shade")
	default:
		fmt.Printf("TODO: osUserCacheDir on GOOS %q", runtime.GOOS)
	}
	return path.Join(dir, "config.json")
}
