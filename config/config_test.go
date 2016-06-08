package config

import (
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/asjoyner/shade/drive"
)

type testSet struct {
	in  []byte
	out []drive.Config
	err string
}

var parsedExample = []drive.Config{
	{
		Provider: "amazon",
		OAuth: drive.OAuthConfig{
			ID:     "12345",
			Secret: "abcde",
			Scope:  "readonly",
		},
		FileParentId:  "1",
		ChunkParentId: "2",
		Write:         false,
	},
}

func TestParseConfig(t *testing.T) {
	// Define tests for the simple failure cases
	tests := []testSet{
		{[]byte{}, nil, "json unmarshal error"},
		{[]byte("[]"), nil, "no provider"},
	}

	// Pull in the example config and parse it too
	example, err := ioutil.ReadFile("../examples/config.json")
	if err != nil {
		t.Errorf("Could not read ../examples/config.json: %s", err)
	} else {
		tests = append(tests, testSet{example, parsedExample, ""})
	}

	for _, ts := range tests {
		configs, err := parseConfig(ts.in)
		if err != nil {
			if ts.err == "" {
				t.Fatalf("parseConfig(%q) returned unexpected error: %q", err)
			} else if !strings.Contains(err.Error(), ts.err) {
				t.Fatalf("parseConfig(%q); got err: %q, want: %q", ts.in, err, ts.err)
			}
		} else {
			if !reflect.DeepEqual(ts.out, configs) {
				t.Fatalf("parseConfig(%q); got: %q, want: %q", ts.in, configs, ts.out)
			}
		}
	}
}
