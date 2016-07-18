package cache

import (
	"testing"

	"github.com/asjoyner/shade/drive"

	_ "github.com/asjoyner/shade/drive/fail"
	"github.com/asjoyner/shade/drive/memory"
	_ "github.com/asjoyner/shade/drive/win"
)

// Test a single pass through to the memory client.
func TestFileRoundTrip(t *testing.T) {
	cc, err := NewClient([]drive.Config{{Provider: "memory", Write: true}})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc)
}

// Test a single pass through to the memory client.
func TestChunkRoundTrip(t *testing.T) {
	cc, err := NewClient([]drive.Config{{Provider: "memory", Write: true}})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestChunkRoundTrip(t, cc)
}

// Test that a client is required.
func TestNoConfigs(t *testing.T) {
	_, err := NewClient(nil)
	if err == nil {
		t.Fatal("NewClient(nil); expected err, got nil")
	}
}

// Test two clients work as expected, and that both end up with the same files
// and chunks.
func TestTwoMemoryClients(t *testing.T) {
	cc, err := NewClient([]drive.Config{
		{Provider: "memory", Write: true},
		{Provider: "memory", Write: true},
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc)
	drive.TestChunkRoundTrip(t, cc)

	// assert the actual class types to be able to check the internals
	client0 := cc.(*Drive).clients[0].(*memory.Drive)
	client1 := cc.(*Drive).clients[1].(*memory.Drive)

	compareMemoryDrives(t, client0, client1)
}

func TestMemoryAndFailClients(t *testing.T) {
	cc, err := NewClient([]drive.Config{
		{Provider: "memory", Write: true},
		{
			Provider: "fail",
			Write:    true,
		},
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	drive.TestFileRoundTrip(t, cc)
	drive.TestChunkRoundTrip(t, cc)
}

func TestOnlyPersistentSatisfies(t *testing.T) {
	cc, err := NewClient([]drive.Config{
		{Provider: "memory", Write: true},
		{
			Provider: "fail",
			OAuth:    drive.OAuthConfig{ClientID: "persistent"},
			Write:    true,
		},
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	if cc.PutFile([]byte("b13s"), []byte("Hope is not a strategy.")) == nil {
		t.Fatal("failed write to only persistent drive did not fail PutFile")
	}
	if cc.PutChunk([]byte("b13s"), []byte("Hope is not a strategy.")) == nil {
		t.Fatal("failed write to only persistent drive did not fail PutChunk")
	}
}

func compareMemoryDrives(t *testing.T, client0, client1 *memory.Drive) {
	files0, _ := client0.ListFiles() // memory client never returns err
	files1, _ := client1.ListFiles() // memory client never returns err

	if len(files0) != len(files1) {
		t.Errorf("backing clients have different numbers of files:\n0: %+v\n1:%+v", len(files0), len(files1))
	}
	compareByteSlices(t, files0, files1)

	chunks0 := client0.ListChunks()
	chunks1 := client1.ListChunks()

	if len(chunks0) != len(chunks1) {
		t.Errorf("backing clients have different numbers of chunks:\n0: %+v\n1:%+v", len(chunks0), len(chunks1))
	}

	compareByteSlices(t, chunks0, chunks1)
}

func compareByteSlices(t *testing.T, a, b [][]byte) {
	// throw them both into a map to compare
	fm := make(map[string]int)
	for _, f := range a {
		fm[string(f)]++
	}
	for _, f := range b {
		fm[string(f)]++
	}
	for f, times := range fm {
		if times == 1 {
			t.Errorf("one client didn't get this file: %x", f)
		}
	}
}
