package shade

import "testing"

func TestUpdateFilesize(t *testing.T) {
	f := File{
		Chunks:        []Chunk{{}, {}, {}},
		Chunksize:     16 * 1024 * 1024,
		LastChunksize: 3,
	}
	f.UpdateFilesize()
	expected := int64((2 * 16 * 1024 * 1024) + 3)
	if f.Filesize != expected {
		t.Errorf("UpdateFilesize unexpected, want: %d, got: %d", f.Filesize, expected)
	}
}
