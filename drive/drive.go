package drive

// Client is a generic interface to a cloud storage backend.
type Client interface {
	// GetFiles retrieves all of the File objects known to the client.
	// The responses are marshalled JSON, which may be encrypted.
	GetFiles() ([]string, error)

	// PutFile writes the metadata describing a new file.
	// f should be marshalled JSON, and may be encrypted.
	PutFile(f string) error

	// GetChunk retrieves a chunk with a given SHA-256 sum
	GetChunk(sha256 []byte) ([]byte, error)

	// PutChunk writes a chunk and returns its SHA-256 sum
	PutChunk(sha256 []byte, chunk []byte) ([]byte, error)
}
