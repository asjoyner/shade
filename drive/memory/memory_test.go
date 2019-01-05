package memory

import (
	"bytes"
	"testing"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

func TestFileRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{
		Provider:      "memory",
		MaxFiles:      500,
		MaxChunkBytes: 100 * 256 * 50,
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, mc, 100)
}

func TestChunkRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{
		Provider:      "memory",
		MaxFiles:      500,
		MaxChunkBytes: 100 * 256 * 50,
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestChunkRoundTrip(t, mc, 100)
}

func TestParallelRoundTrip(t *testing.T) {
	mc, err := NewClient(drive.Config{
		Provider:      "memory",
		MaxFiles:      10000,
		MaxChunkBytes: 10000 * 256 * 50,
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestParallelRoundTrip(t, mc, 100)
}

func TestChunkLister(t *testing.T) {
	mc, err := NewClient(drive.Config{
		Provider:      "memory",
		MaxFiles:      10000,
		MaxChunkBytes: 10000 * 256 * 50,
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	drive.TestChunkLister(t, mc, 100)
}

func TestComparingEqualLRUs(t *testing.T) {
	a, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	a.PutFile([]byte{1}, []byte{1, 2, 3})
	a.PutChunk([]byte{2}, []byte{4, 5, 6}, nil)
	b, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	b.PutFile([]byte{1}, []byte{1, 2, 3})
	b.PutChunk([]byte{2}, []byte{4, 5, 6}, nil)
	if err := a.(*Drive).Equal(b.(*Drive)); err != nil {
		t.Error(err)
	}
}

func TestComparingUnequalLRUs(t *testing.T) {
	a, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	a.PutFile([]byte{1}, []byte{1, 2, 3})
	a.PutChunk([]byte{2}, []byte{4, 5, 6}, nil)

	b, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	b.PutFile([]byte{1}, []byte{1, 2, 3})
	if err := a.(*Drive).Equal(b.(*Drive)); err == nil {
		t.Errorf("did not detect clients with unequal number of chunks")
	}

	c, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	c.PutChunk([]byte{2}, []byte{4, 5, 6}, nil)
	if err := a.(*Drive).Equal(b.(*Drive)); err == nil {
		t.Errorf("did not detect clients with unequal number of chunks")
	}

	d, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	d.PutFile([]byte{1}, []byte{})
	d.PutChunk([]byte{2}, []byte{4, 5, 6}, nil)
	if err := a.(*Drive).Equal(d.(*Drive)); err == nil {
		t.Errorf("did not detect clients with unequal file values")
	}

	e, err := NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	e.PutFile([]byte{1}, []byte{1, 2, 3})
	e.PutChunk([]byte{2}, []byte{}, nil)
	if err := a.(*Drive).Equal(e.(*Drive)); err == nil {
		t.Errorf("did not detect clients with unequal file values")
	}
}

func TestReturnsAreCopied(t *testing.T) {
	mc, err := NewClient(drive.Config{
		Provider:      "memory",
		MaxFiles:      500,
		MaxChunkBytes: 100 * 256 * 50,
	})
	if err != nil {
		t.Fatalf("NewClient() for test config failed: %s", err)
	}
	golden := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	sum := shade.Sum(data)
	if err := mc.PutFile(sum, data); err != nil {
		t.Fatalf("could not write to client: %s", err)
	}
	respA, err := mc.GetFile(sum)
	if err != nil {
		t.Fatalf("could not write to client: %s", err)
	}

	// change a byte in the returned byte array
	respA[2] = '!'

	// Ask for the same bytes again
	respB, err := mc.GetFile(sum)
	if err != nil {
		t.Fatalf("could not write to client: %s", err)
	}

	if !bytes.Equal(respB, golden) {
		t.Fatalf("modyfing files returned by the client modifies the cache!")
	}

	// Repeate the procedure for Chunks
	data = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	sum = shade.Sum(data)
	if err := mc.PutChunk(sum, data, nil); err != nil {
		t.Fatalf("could not write to client: %s", err)
	}
	respA, err = mc.GetChunk(sum, nil)
	if err != nil {
		t.Fatalf("could not write to client: %s", err)
	}

	// change a byte in the returned byte array
	respA[1] = '!'

	// Ask for the same bytes again
	respB, err = mc.GetChunk(sum, nil)
	if err != nil {
		t.Fatalf("could not write to client: %s", err)
	}

	if !bytes.Equal(respB, golden) {
		t.Fatalf("modyfing chunks returned by the client modifies the cache!")
	}
}
