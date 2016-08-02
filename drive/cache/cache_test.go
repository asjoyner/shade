package cache

import (
	"testing"
	"time"

	"github.com/asjoyner/shade/drive"

	"github.com/asjoyner/shade/drive/fail"
	"github.com/asjoyner/shade/drive/memory"
)

// Test a single pass through to the memory client.
func TestRoundTrip(t *testing.T) {
	cc, err := NewClient([]drive.Client{memClient(t)})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc)
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
	cc, err := NewClient([]drive.Client{
		memClient(t),
		memClient(t),
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, cc)
	drive.TestChunkRoundTrip(t, cc)

	// assert the actual class types to be able to check the internals
	client0 := cc.clients[0].(*memory.Drive)
	client1 := cc.clients[1].(*memory.Drive)

	// Give the myriad goroutines a second to coalesce
	time.Sleep(1 * time.Second)

	compareMemoryDrives(t, client0, client1)
}

func TestMemoryAndFailClients(t *testing.T) {
	failClient, _ := fail.NewClient(drive.Config{
		Provider: "fail",
		Write:    true,
	})
	cc, err := NewClient([]drive.Client{memClient(t), failClient})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}

	drive.TestFileRoundTrip(t, cc)
	drive.TestChunkRoundTrip(t, cc)
}

func TestOnlyPersistentSatisfies(t *testing.T) {
	failClient, _ := fail.NewClient(drive.Config{
		Provider: "fail",
		OAuth:    drive.OAuthConfig{ClientID: "persistent"},
		Write:    true,
	})
	cc, err := NewClient([]drive.Client{memClient(t), failClient})
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

func memClient(t *testing.T) drive.Client {
	mc, err := memory.NewClient(drive.Config{Provider: "memory", Write: true})
	if err != nil {
		t.Fatalf("could not initialize memory client: %s", err)
	}
	return mc
}
