package shade

type File struct {
	Filename  string
	Chunksize int
	Chunks    []Chunk
	AesKey    []byte
}

type Chunk struct {
	Index  int
	Sha256 []byte
}
