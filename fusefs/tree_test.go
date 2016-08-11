package fusefs

import (
	"testing"

	"github.com/asjoyner/shade/drive"
	_ "github.com/asjoyner/shade/drive/win"
)

func TestSynthetic(t *testing.T) {
	tests := []struct {
		sha  []byte
		want bool
	}{
		{
			nil,
			true,
		},
		{
			[]byte{1},
			false,
		},
	}
	for _, test := range tests {
		n := Node{
			Sha256sum: test.sha,
		}
		if got := n.Synthetic(); got != test.want {
			t.Fatalf("Synthetic() == %v; want %v", got, test.want)
		}
	}
}

func TestNodeByPath(t *testing.T) {
	tests := []struct {
		nodes   map[string]Node
		wantErr bool
	}{
		{
			map[string]Node{"n": {}},
			false,
		},
		{
			map[string]Node{},
			true,
		},
	}
	for _, test := range tests {
		tree := Tree{nodes: test.nodes}
		_, err := tree.NodeByPath("n")
		if err == nil && test.wantErr {
			t.Fatalf("NodeByPath(\"n\") did not return expected error")
		}
		if err != nil && !test.wantErr {
			t.Fatalf("NodeByPath(\"n\") returned unexpected error")
		}
	}
}

func TestAddParent(t *testing.T) {
	ts := map[string]map[string]string{
		"foo/bar/baz": map[string]string{
			"":        "foo",
			"foo":     "bar",
			"foo/bar": "baz",
		},
		"foo/bar/baz/rosencrantz/guildenstern": map[string]string{
			"":                        "foo",
			"foo":                     "bar",
			"foo/bar":                 "baz",
			"foo/bar/baz":             "rosencrantz",
			"foo/bar/baz/rosencrantz": "guildenstern",
		},
		"particularlylongname/shortishname": map[string]string{
			"": "particularlylongname",
			"particularlylongname": "shortishname",
		},
	}

	for path, pairs := range ts {
		client, err := drive.NewClient(drive.Config{Provider: "win"})
		if err != nil {
			t.Fatalf("failed to initialize test client: %s", err)
		}
		tree, err := NewTree(client, nil)
		if err != nil {
			t.Fatalf("failed to initialize Tree: %s", err)
		}
		tree.addParents(path)
		for parent, child := range pairs {
			var found bool
			for c := range tree.nodes[parent].Children {
				if c == child {
					found = true
				}
			}
			if !found {
				t.Errorf("path %s, %s is not a child of %s", path, child, parent)
			}
		}
	}
}
