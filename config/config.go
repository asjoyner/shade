package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/asjoyner/shade"
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
	for _, config := range configs {
		if !drive.ValidProvider(config.Provider) {
			return nil, fmt.Errorf("unsupported provider in config: %q", config.Provider)
		}
	}
	return configs, nil
}

// configPath returns the path of the JSON config file.
func configPath() string {
	return path.Join(shade.ConfigDir(), "config.json")
}

func Clients() ([]drive.Client, error) {
	configs, err := Read()
	if err != nil {
		return nil, fmt.Errorf("could not parse config: %s", err)
	}

	// initialize the drive client(s)
	var clients []drive.Client
	for _, conf := range configs {
		c, err := drive.NewClient(conf)
		if err != nil {
			return nil, fmt.Errorf("%s: %s", conf.Provider, err)
		}
		clients = append(clients, c)
	}
	return clients, nil
}
