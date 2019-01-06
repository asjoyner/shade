package compare

import (
	"testing"

	"github.com/asjoyner/shade/drive"
	_ "github.com/asjoyner/shade/drive/memory"
)

func TestEqualClients(t *testing.T) {
	a := newMemClient(t)
	b := newMemClient(t)
	testFiles := drive.RandChunks(10)
	testChunks := drive.RandChunks(10)

	put(t, a, testFiles, testChunks)
	put(t, b, testFiles, testChunks)

	eq, err := Equal(a, b)
	if err != nil {
		t.Fatalf("Failed to compare clients: %s", err)
	}
	if !eq {
		t.Errorf("client is not Equal() to itself!")
	}
}

func TestEqualAndGetDeltaFindDifferences(t *testing.T) {
	a := newMemClient(t)
	testFiles1 := drive.RandChunks(2)
	testChunks1 := drive.RandChunks(2)
	put(t, a, testFiles1, testChunks1)

	b := newMemClient(t)
	testFiles2 := drive.RandChunks(2)
	testChunks2 := drive.RandChunks(2)
	put(t, b, testFiles2, testChunks2)

	eq, err := Equal(a, b)
	if err != nil {
		t.Fatalf("Failed to compare clients: %s", err)
	}
	if eq {
		t.Errorf("Equal() did not detect difference in two clients")
	}

	aDelta, bDelta, err := GetDelta(a, b)

	testSet := []struct {
		name string
		want map[string][]byte
		got  [][]byte
	}{
		{
			name: "aDelta.Files",
			want: testFiles1,
			got:  aDelta.Files,
		},
		{
			name: "aDelta.Chunks",
			want: testChunks1,
			got:  aDelta.Chunks,
		},
		{
			name: "bDelta.Files",
			want: testFiles2,
			got:  bDelta.Files,
		},
		{
			name: "bDelta.Chunks",
			want: testChunks2,
			got:  bDelta.Chunks,
		},
	}
	for _, ts := range testSet {
		for _, got := range ts.got {
			if _, ok := ts.want[string(got)]; !ok {
				t.Errorf("%s contains extra value: %x", ts.name, got)
			}
			delete(ts.want, string(got))
		}
		t.Log(len(ts.want))
		for want := range ts.want {
			t.Errorf("%s incorrect, missing: %x", ts.name, want)
		}
	}

}

// put is a test helper function which populates data into the provided client.
// If it encounters errors, it calls t.Fatalf()
func put(t *testing.T, c drive.Client, files, chunks map[string][]byte) {
	// Populate some files into the client
	for stringSum, file := range files {
		if err := c.PutFile([]byte(stringSum), []byte(file)); err != nil {
			t.Fatal("Failed to put test file: ", err)
		}
	}
	// Populate some chunks into the client
	for stringSum, file := range chunks {
		if err := c.PutChunk([]byte(stringSum), []byte(file), nil); err != nil {
			t.Fatal("Failed to put test chunk: ", err)
		}
	}
}

// newMemClient is a test helper function which returns a basic memory client.
// If it encounters errors, it calls t.Fatalf()
func newMemClient(t *testing.T) drive.Client {
	c, err := drive.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
