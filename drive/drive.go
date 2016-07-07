package drive

// Client is a generic interface to a cloud storage backend.
type Client interface {
	// ListFiles retrieves all of the File objects known to the client.  The return
	// maps from arbitrary unique keys to the sha256sum of the file object.  The
	// keys may be passed to GetFile() to retrieve the corresponding shade.File.
	ListFiles() (map[string][]byte, error)

	// GetFile retrieves the File object described by the ID
	// The response is marshalled JSON, which may be encrypted.
	GetFile(fileID string) ([]byte, error)

	// GetChunk retrieves a chunk with a given SHA-256 sum
	GetChunk(sha256 []byte) ([]byte, error)

	// PutFile writes the metadata describing a new file.
	// f should be marshalled JSON, and may be encrypted.  It differs only from
	// PutChunk in that ListFiles() will return these chunks.
	PutFile(sha256, chunk []byte) error

	// PutChunk writes a chunk and returns its SHA-256 sum
	PutChunk(sha256, chunk []byte) error
}

type Config struct {
	Provider      string
	OAuth         OAuthConfig
	FileParentID  string
	ChunkParentID string
	Write         bool
}

type OAuthConfig struct {
	ClientID     string
	ClientSecret string
	Scopes       []string
	TokenPath    string
}
