package shade

import "fmt"

type File struct {
	Filename  string
	Filesize  int64
	Chunksize int
	Chunks    []Chunk
	AesKey    []byte
}

type Chunk struct {
	Index  int
	Sha256 []byte
}

func (f *File) String() string {
	out := fmt.Sprintf("{Filename: %s, Filesize: %d, Chunksize: %d,", f.Filename, f.Filesize, f.Chunksize)
	sep := ",\n"
	if len(f.Chunks) < 2 {
		sep = ", "
	}
	for _, c := range f.Chunks {
		out += c.String() + sep
	}
	return out
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{Index: %d, Sha256: %x}", c.Index, c.Sha256)
}
