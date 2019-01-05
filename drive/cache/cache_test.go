package cache

import (
	"testing"

	"github.com/asjoyner/shade/drive"

	_ "github.com/asjoyner/shade/drive/fail"
	"github.com/asjoyner/shade/drive/memory"
)

// Test a single pass through to the memory client.
func TestRoundTrip(t *testing.T) {
	cc, err := NewClient(drive.Config{
		Children: []drive.Config{
			drive.Config{Provider: "memory", Write: true},
		},
	},
	)
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc, 100)
	drive.TestChunkRoundTrip(t, cc, 100)
}

// Test that a client is required.
func TestNoConfigs(t *testing.T) {
	_, err := NewClient(drive.Config{})
	if err == nil {
		t.Fatal("NewClient(nil); expected err, got nil")
	}
}

// Test two clients work as expected, and that both end up with the same files
// and chunks.
func TestTwoMemoryClients(t *testing.T) {
	cc, err := NewClient(drive.Config{
		Children: []drive.Config{
			drive.Config{Provider: "memory", Write: true},
			drive.Config{Provider: "memory", Write: true},
		},
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc, 100)
	drive.TestChunkRoundTrip(t, cc, 100)

	// assert the actual class types to be able to check the internals
	cacheClient := cc.(*Drive)
	client0 := cacheClient.clients[0].(*memory.Drive)
	client1 := cacheClient.clients[1].(*memory.Drive)

	if err := client0.Equal(client1); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryAndFailClients(t *testing.T) {
	cc, err := NewClient(drive.Config{
		Children: []drive.Config{
			drive.Config{Provider: "memory", Write: true},
			drive.Config{Provider: "fail", Write: true},
		},
	},
	)
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	drive.TestFileRoundTrip(t, cc, 100)
	drive.TestChunkRoundTrip(t, cc, 100)
}

func TestOnlyPersistentSatisfies(t *testing.T) {
	cc, err := NewClient(drive.Config{
		Children: []drive.Config{
			{Provider: "memory", Write: true},
			{
				Provider: "fail",
				OAuth:    drive.OAuthConfig{ClientID: "persistent"},
				Write:    true,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	if cc.PutFile([]byte("b13s"), []byte("Hope is not a strategy.")) == nil {
		t.Fatal("failed write to only persistent drive did not fail PutFile")
	}
	if cc.PutChunk([]byte("b13s"), []byte("Hope is not a strategy."), nil) == nil {
		t.Fatal("failed write to only persistent drive did not fail PutChunk")
	}
}
