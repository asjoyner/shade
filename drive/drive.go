package drive

import (
	"fmt"
	"sync"

	"github.com/asjoyner/shade"
)

// Client is a generic interface to a cloud storage backend.
type Client interface {
	// ListFiles retrieves the sha256sum of all of the File objects known to the
	// client.  The elements of the slice may be passed to GetChunk() to retrieve
	// the corresponding shade.File object.  It will be marshaled JSON,
	// optionally encrypted.
	ListFiles() ([][]byte, error)

	// GetFile retrieves the metadata describing a shade.File.
	//
	// f should be marshalled JSON, and may be encrypted.  It differs only from
	// PutChunk in that ListFiles() will return these chunks.  sha256 is the hash
	// of the unencrypted shade.File object.  Nb: this leaks the a bit of
	// information about the file, but given that it contains the AES key, the
	// mtime of the shade.File, etc it should contain enough arbitrary data that
	// you can't infer anything meaningful from the SHA sum.
	GetFile(sha256 []byte) ([]byte, error)

	// PutFile writes the metadata describing a new file.
	// f should be marshalled JSON, and may be encrypted.  It differs only from
	// PutChunk in that ListFiles() will return these chunks.
	PutFile(sha256, chunk []byte) error

	// ReleaseFile indicates this file is no longer required.
	//
	// The file is not required to be deleted by the client.
	ReleaseFile(sha256 []byte) error

	// ListChunks provides an iterator to return each chunk known to the client.
	NewChunkLister() ChunkLister

	// GetChunk retrieves a chunk with a given SHA-256 sum.  f is required for
	// files to support encryption.  It is used to store the AES key the chunk
	// and chunksum are encrypted with.
	GetChunk(sha256 []byte, f *shade.File) ([]byte, error)

	// PutChunk writes a chunk and returns its SHA-256 sum.  f is required for
	// files to support encryption.  It is used to store the AES key the chunk
	// and chunksum are encrypted with.
	PutChunk(sha256, chunk []byte, f *shade.File) error

	// ReleaseChunk indicates this chunk is no longer required.
	//
	// The chunk is not required to be deleted by the client.
	ReleaseChunk(sha256 []byte) error

	// Warm is an optional hint to clients that the supplied chunks might be
	// fetched soon.  This helps batch up metadata queries for remote clients, to
	// reduce their latency impact.  Most local clients can return nil.
	Warm(chunks [][]byte, file *shade.File)

	// GetConfig returns the drive.Config object used to initialize this client.
	// This is mostly helpful for debugging, to identify which Provider it is.
	GetConfig() Config

	// Local identifies the storage destination of the client to the caller.
	// If it returns false, code can expect that the content of this storage will
	// persist after the death of the binary, or the machine on which it is
	// running, or the continent on which it is located suffering a high altitude
	// EMP burst.
	Local() bool

	// Persistent identifies the storage durability of the client to the caller.
	// If it returns false, code can expect that the content of this storage will
	// persist after the death of the binary, but perhaps not the machine on
	// which it is running
	Persistent() bool
}

// ChunkLister provides a mechanism to iterate the Sha256 sums of all the
// chunks in a Drive.  It uses a different pattern from ListFiles because
// there may be a prohibitively large number of chunk sums to return all at
// once.
type ChunkLister interface {
	// Next prepares the next chunk sum for reading with the Sha256 method. It
	// returns true on success, or false if there are no more sums or an error
	// happened. Err should be consulted to distinguish between the two cases.
	Next() bool
	// Sum returns the SHA256 sum of a chunk known to the client.
	Sha256() []byte
	// Err returns the error, if any, that was encountered during iteration.
	Err() error
}

// Config contains the configuration for the cloud drive being accessed.
type Config struct {
	Provider      string
	OAuth         OAuthConfig
	FileParentID  string
	ChunkParentID string
	Write         bool
	MaxFiles      uint64
	MaxChunkBytes uint64

	// See the godoc for the "encrypt" package for more details.
	// Tip: `shadeutil genkeys -t N` will generate RSA keys and print them as
	// properly formatted JSON strings, to make it easier to format a Config for
	// the "encrypt" client.
	RsaPublicKey  string
	RsaPrivateKey string

	Children []Config
}

// OAuthConfig contains the OAuth configuration information.
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
