package drive

import (
	"fmt"
	"sync"
)

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

type clientCreator func(c Config) (Client, error)

var (
	mu        sync.RWMutex // Protects providers.
	providers = make(map[string]clientCreator)
)

// RegisterProvider declares that a provider with a given name exists and can
// be used via the calls below.
func RegisterProvider(name string, f clientCreator) {
	mu.Lock()
	defer mu.Unlock()
	providers[name] = f
}

// ValidProvider indicates whether a provider with the given name is registered.
func ValidProvider(name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, valid := providers[name]
	return valid
}

// NewClient creates a new client of type provider with the provided config.
func NewClient(c Config) (Client, error) {
	mu.RLock()
	defer mu.RUnlock()
	if f, ok := providers[c.Provider]; ok {
		return f(c)
	}
	return nil, fmt.Errorf("unknown provider: %q", c.Provider)
}
