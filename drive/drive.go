package drive

// Client is a generic interface to a cloud storage backend.
type Client interface {
	// ListFiles retrieves the sha256sum of all of the File objects known to the
	// client.  The elements of the slice may be passed to GetChunk() to retrieve
	// the corresponding shade.File object.  It will be marshaled JSON,
	// optionally encrypted.
	ListFiles() ([][]byte, error)

	// GetChunk retrieves a chunk with a given SHA-256 sum
	GetChunk(sha256 []byte) ([]byte, error)

	// PutFile writes the metadata describing a new file.
	// f should be marshalled JSON, and may be encrypted.  It differs only from
	// PutChunk in that ListFiles() will return these chunks.
	PutFile(sha256, chunk []byte) error

	// PutChunk writes a chunk and returns its SHA-256 sum
	PutChunk(sha256, chunk []byte) error

	// GetConfig returns the drive.Config object used to initialize this client.
	// This is mostly helpful for debugging, to identify which Provider it is.
	GetConfig() Config

	// Local identifies the storage destination of the client to the caller.
	// If it returns false, code can expect that the content of this storage will
	// persist after the death of the binary, or the machine on which it is
	// running, or the continent on which it is located suffering a high altitude
	// EMP burst.
	Local() bool
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
