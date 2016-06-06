package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"

	"github.com/asjoyner/shade/drive"
)

func ReadConfig() ([]drive.Config, error) {
	filename := ConfigPath()
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("ReadFile(%q): %s", filename, err)
	}

	var configs []drive.Config
	if err := json.Unmarshal(contents, &configs); err != nil {
		return nil, fmt.Errorf("parsing %q: %s", filename, err)
	}
	return configs, nil
}

func ConfigPath() string {
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
