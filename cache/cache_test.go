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
