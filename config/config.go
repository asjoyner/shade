package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/asjoyner/shade/drive"
)

// Read finds, reads, parses, and returns the config.
func Read(filename string) (drive.Config, error) {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return drive.Config{}, fmt.Errorf("ReadFile(%q): %s", filename, err)
	}

	configs, err := parseConfig(contents)
	if err != nil {
		return drive.Config{}, fmt.Errorf("parsing %q: %s", filename, err)
	}

	return configs, nil
}

// parseConfig is broken out primarily to test unmarshaling of various example
// configuration objects.
func parseConfig(contents []byte) (drive.Config, error) {
	var config drive.Config
	if err := json.Unmarshal(contents, &config); err != nil {
		return drive.Config{}, fmt.Errorf("json unmarshal error: %s", err)
	}
	if !drive.ValidProvider(config.Provider) {
		return drive.Config{}, fmt.Errorf("unsupported provider in config: %q", config.Provider)
	}
	return config, nil
}
