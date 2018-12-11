/*Package google provides a Shade storage implementation for Google Drive.

You may optionally configure a FileParentID and ChunkParentID to indicate where
to store the files and chunks.  These values are Drive's alphanumeric unique
identifiers for directories.  You can find the ID for a directory in the URL
when viewing the file in the Google Drive web UI.  These can be set to the same
value, and AppProperties will be used to disambiguate files from chunks

To store Files and Chunks as AppData storage, so that they are not visible in
the Google Drive web UI, set FileParentID and ChunkParentID to 'appDataFolder'.
You can optionally reduce the scope to only
'https://www.googleapis.com/auth/drive.appfolder'.

The following configuration values are not directly supported:
   MaxFiles
   MaxChunkBytes
   RsaPublicKey
   RsaPrivateKey
   Children

To encrypt the contents written to Google Drive, wrap the configuration stanza
with the github.com/asjoyner/shade/drive/encrypt package.

This package supports overriding all of the OAuth configuration parameters.
*/
package google

import (
	"bytes"
	"encoding/hex"
	"expvar"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/golang/glog"

	gdrive "google.golang.org/api/drive/v3"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"

	"golang.org/x/net/context"
)

var (
	listFileReq           = expvar.NewInt("googleListFilesReq")
	getFileReq            = expvar.NewInt("googleGetFileReq")
	putFileReq            = expvar.NewInt("googlePutFileReq")
	getChunkReq           = expvar.NewInt("googleGetChunkReq")
	putChunkReq           = expvar.NewInt("googlePutChunkReq")
	getChunkSuccess       = expvar.NewInt("googleGetChunkSuccess")
	getChunkDupeError     = expvar.NewInt("googleGetDupeError")
	getChunkMetadataError = expvar.NewInt("googleGetChunkMetadataError")
	getChunkDownloadError = expvar.NewInt("googleGetChunkDownloadError")
)

func init() {
	drive.RegisterProvider("google", NewClient)
}

// NewClient returns a new Drive client.
func NewClient(c drive.Config) (drive.Client, error) {
	client := getOAuthClient(c)
	service, err := gdrive.New(client)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Google Drive Client: %v", err)
	}
	return &Drive{
		service: service,
		config:  c,
		files:   make(map[string]string),
	}, nil
}

// Drive represents access to the Google Drive storage system.
type Drive struct {
	service *gdrive.Service
	config  drive.Config

	mu    sync.RWMutex // protects following members
	files map[string]string
}

// ListFiles retrieves all of the File objects known to the client, and returns
// the corresponding sha256sum of the file object.  Those may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	listFileReq.Add(1)
	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	// this query is a Google Drive API query string which will return all
	// shade metadata files, optionally restricted to a FileParentID
	q := "appProperties has { key='shadeType' and value='file' }"
	if s.config.FileParentID != "" {
		q = fmt.Sprintf("%s and '%s' in parents", q, s.config.FileParentID)
	}
	r, err := s.service.Files.List().IncludeTeamDriveItems(true).SupportsTeamDrives(true).Context(ctx).Q(q).Fields("files(id, name)").Do()
	if err != nil {
		glog.Errorf("List(): %v", err)
		return nil, fmt.Errorf("couldn't retrieve files: %v", err)
	}
	s.mu.Lock()
	for _, f := range r.Files {
		// If decoding the name fails, skip the file.
		if b, err := hex.DecodeString(f.Name); err == nil {
			s.files[string(b)] = f.Id
		}
	}
	s.mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()
	resp := make([][]byte, 0, len(s.files))
	for sha256sum := range s.files {
		resp = append(resp, []byte(sha256sum))
	}
	return resp, nil
}

// GetFile retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	getFileReq.Add(1)
	return s.GetChunk(sha256sum, nil)
}

// PutFile writes the metadata describing a new file.
// content should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, content []byte) error {
	putFileReq.Add(1)
	f := &gdrive.File{
		Name:          hex.EncodeToString(sha256sum),
		AppProperties: map[string]string{"shadeType": "file"},
	}
	if s.config.FileParentID != "" {
		f.Parents = []string{s.config.FileParentID}
	}

	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	br := bytes.NewReader(content)
	if _, err := s.service.Files.Create(f).SupportsTeamDrives(true).Context(ctx).Media(br).Do(); err != nil {
		glog.Warningf("couldn't create file: %v", err)
		return fmt.Errorf("couldn't create file: %v", err)
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *Drive) GetChunk(sha256sum []byte, _ *shade.File) ([]byte, error) {
	getChunkReq.Add(1)
	s.mu.RLock()
	fileID, ok := s.files[string(sha256sum)]
	s.mu.RUnlock()

	filename := hex.EncodeToString(sha256sum)
	if !ok {
		ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
		q := fmt.Sprintf("name = '%s'", filename)
		if s.config.FileParentID != "" {
			q = fmt.Sprintf("%s and ('%s' in parents OR '%s' in parents)", q, s.config.FileParentID, s.config.ChunkParentID)
		}
		r, err := s.service.Files.List().SupportsTeamDrives(true).IncludeTeamDriveItems(true).Context(ctx).Q(q).Fields("files(id, name)").Do()
		if err != nil {
			getChunkMetadataError.Add(1)
			glog.Warningf("couldn't get metadata for chunk %v: %v", filename, err)
			return nil, fmt.Errorf("couldn't get metadata for chunk %v: %v", filename, err)
		}
		if len(r.Files) != 1 {
			getChunkDupeError.Add(1)
			glog.Warningf("got non-unique chunk result for chunk %v: %#v", filename, r.Files)
			return nil, fmt.Errorf("got non-unique chunk result for chunk %v: %#v", filename, r.Files)
		}
		fileID = r.Files[0].Id
	}

	resp, err := s.service.Files.Get(fileID).SupportsTeamDrives(true).Download()
	if err != nil {
		getChunkDownloadError.Add(1)
		glog.Warningf("couldn't download chunk %v: %v", filename, err)
		return nil, fmt.Errorf("couldn't download chunk %v: %v", filename, err)
	}
	defer resp.Body.Close()

	chunk, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Warningf("couldn't read chunk %v: %v", filename, err)
		return nil, fmt.Errorf("couldn't read chunk %v: %v", filename, err)
	}
	getChunkSuccess.Add(1)
	return chunk, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256sum, content []byte, _ *shade.File) error {
	putChunkReq.Add(1)
	s.mu.RLock()
	_, ok := s.files[string(sha256sum)]
	s.mu.RUnlock()
	if ok {
		return nil // we know this chunk already exists
	}
	f := &gdrive.File{
		Name:          hex.EncodeToString(sha256sum),
		AppProperties: map[string]string{"shadeType": "chunk"},
	}
	if s.config.ChunkParentID != "" {
		f.Parents = []string{s.config.ChunkParentID}
	}

	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	br := bytes.NewReader(content)
	if _, err := s.service.Files.Create(f).SupportsTeamDrives(true).Context(ctx).Media(br).Do(); err != nil {
		glog.Warningf("couldn't create file: %v", err)
		return fmt.Errorf("couldn't create file: %v", err)
	}
	return nil
}

// GetConfig returns the associated drive.Config object.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether access is local.
func (s *Drive) Local() bool { return false }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return true }
