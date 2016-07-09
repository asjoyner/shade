package shade

import (
	"fmt"
	"time"
)

type File struct {
	Filename     string
	Filesize     int64
	ModifiedTime time.Time
	Chunksize    int
	Chunks       []Chunk
	AesKey       []byte
}

type Chunk struct {
	Index  int
	Sha256 []byte
}

func (f *File) String() string {
	out := fmt.Sprintf("{Filename: %q, Filesize: %d, Chunksize: %d, AesKey: %q, Chunks:", f.Filename, f.Filesize, f.Chunksize, f.AesKey)
	sep := ", "
	if len(f.Chunks) < 2 {
		out += " "
	} else {
		out += "\n"
		sep = ",\n"
	}
	for i, c := range f.Chunks {
		if i == len(f.Chunks) {
			out += c.String() + sep
		} else {
			out += c.String()
		}
	}
	return out
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{Index: %d, Sha256: %x}", c.Index, c.Sha256)
}
