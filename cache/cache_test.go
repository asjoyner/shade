package cache

import (
	"testing"
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
		b := Node{
			Sha256sum: test.sha,
		}
		got := b.Synthetic()
		if got != test.want {
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
		r := Reader{nodes: test.nodes}
		_, err := r.NodeByPath("n")
		if err == nil && test.wantErr {
			t.Fatalf("NodeByPath(\"n\") did not return expected error")
		}
		if err != nil && !test.wantErr {
			t.Fatalf("NodeByPath(\"n\") returned unexpected error")
		}
	}
}
