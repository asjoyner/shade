package memory

import (
	"testing"

	"github.com/asjoyner/shade/drive"
)

func TestFileRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, mc)
}

func TestChunkRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestChunkRoundTrip(t, mc)
}

func TestParallelRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestParallelRoundTrip(t, mc)
}
