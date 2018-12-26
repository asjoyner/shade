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
	"errors"
	"expvar"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/golang/glog"

	gdrive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"

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
	getChunkMissing       = expvar.NewInt("googleGetMissing")
	getChunkMetadataError = expvar.NewInt("googleGetChunkMetadataError")
	getChunkDownloadError = expvar.NewInt("googleGetChunkDownloadError")
)

func init() {
	drive.RegisterProvider("google", NewClient)
}

// NewClient returns a new Drive client.
func NewClient(c drive.Config) (drive.Client, error) {
	client := GetOAuthClient(c)
	service, err := gdrive.New(client)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Google Drive Client: %v", err)
	}
	return &Drive{service: service, config: c}, nil
}

// Drive represents access to the Google Drive storage system.
type Drive struct {
	service *gdrive.Service
	config  drive.Config
}

// ListFiles retrieves all of the File objects known to the client, and returns
// the corresponding sha256sum of the file object.  Those may be passed to
// GetChunk() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	listFileReq.Add(1)
	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	resp := make([][]byte, 0)
	// this query is a Google Drive API query string which will return all
	// shade metadata files, optionally restricted to a FileParentID
	q := "appProperties has { key='shadeType' and value='file' }"
	if s.config.FileParentID != "" {
		q = fmt.Sprintf("%s and '%s' in parents", q, s.config.FileParentID)
	}
	req := s.service.Files.List()
	req = req.Context(ctx).Q(q).Fields("files(id, name)")
	req = req.IncludeTeamDriveItems(true).SupportsTeamDrives(true)
	req = req.Corpora("user,allTeamDrives")
	r, err := req.Do()
	if err != nil {
		glog.Errorf("List(): %v", err)
		return nil, fmt.Errorf("couldn't retrieve files: %v", err)
	}
	for _, f := range r.Files {
		// If decoding the name fails, skip the file.
		if b, err := hex.DecodeString(f.Name); err == nil {
			resp = append(resp, b)
		}
	}
	return resp, nil
}

// GetFile retrieves a chunk with a given SHA-256 sum.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	getFileReq.Add(1)
	return s.retrieve(sha256sum)
}

// PutFile writes the metadata describing a new file.
// content should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, content []byte) error {
	putFileReq.Add(1)
	f := &gdrive.File{
		Name:          hex.EncodeToString(sha256sum),
		AppProperties: map[string]string{"shadeType": "file"},
		Properties:    map[string]string{"zb": hex.EncodeToString(content[0:1])},
	}
	if s.config.FileParentID != "" {
		f.Parents = []string{s.config.FileParentID}
	}

	// Avoid the Google Drive API dividing the upload into smaller chunks, and
	// having to detect the content type.
	opts := []googleapi.MediaOption{
		googleapi.ChunkSize(len(content)),
		googleapi.ContentType("application/javascript"),
	}

	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	br := bytes.NewReader(content)
	if _, err := s.service.Files.Create(f).SupportsTeamDrives(true).Context(ctx).Media(br, opts...).Do(); err != nil {
		glog.Warningf("couldn't create file: %v", err)
		return fmt.Errorf("couldn't create file: %v", err)
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum.
func (s *Drive) GetChunk(sha256sum []byte, _ *shade.File) ([]byte, error) {
	getChunkReq.Add(1)
	return s.retrieve(sha256sum)
}

// retrieve is the internal implementation that fetches bytes by sha256sum.  It
// is called by both GetFile and GetChunk.
func (s *Drive) retrieve(sha256sum []byte) ([]byte, error) {
	glog.V(3).Infof("Fetching %x", sha256sum)
	start := time.Now()
	filename := hex.EncodeToString(sha256sum)

	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	q := fmt.Sprintf("name = '%s'", filename)
	if s.config.FileParentID != "" {
		q = fmt.Sprintf("%s and ('%s' in parents OR '%s' in parents)", q, s.config.FileParentID, s.config.ChunkParentID)
	}
	req := s.service.Files.List()
	req = req.Context(ctx).Q(q).Fields("files(id, name, properties, size)")
	req = req.SupportsTeamDrives(true).IncludeTeamDriveItems(true)
	req = req.Corpora("user,allTeamDrives")
	resp, err := req.Do()
	if err != nil {
		getChunkMetadataError.Add(1)
		glog.Warningf("couldn't get metadata for chunk %v: %v", filename, err)
		return nil, fmt.Errorf("couldn't get metadata for chunk %v: %v", filename, err)
	}
	if len(resp.Files) == 0 {
		getChunkMissing.Add(1)
		glog.Warningf("got request for missing chunk %v: %#v", filename, resp)
		return nil, fmt.Errorf("got request for missing chunk %v", filename)
	}
	if len(resp.Files) > 1 {
		getChunkDupeError.Add(1)
		glog.Warningf("got non-unique chunk result for chunk %v: %#v", filename, resp.Files)
		return nil, fmt.Errorf("got non-unique chunk result for chunk %v: %#v", filename, resp.Files)
	}
	file := resp.Files[0]

	dlReq := s.service.Files.Get(file.Id).SupportsTeamDrives(true)

	zb, err := getZerobyte(file)
	if err != nil {
		glog.Warningf("getZerobyte(%s): %s", file.Name, err)
	} else {
		dlReq.Header().Add("Range", fmt.Sprintf("bytes=1-%d", file.Size))
	}

	dlResp, err := dlReq.Download()
	if err != nil {
		getChunkDownloadError.Add(1)
		glog.Warningf("couldn't download chunk %v: %v", filename, err)
		return nil, fmt.Errorf("couldn't download chunk %v: %v", filename, err)
	}
	defer dlResp.Body.Close()

	chunk, err := ioutil.ReadAll(dlResp.Body)
	if err != nil {
		glog.Warningf("couldn't read chunk %v: %v", filename, err)
		return nil, fmt.Errorf("couldn't read chunk %v: %v", filename, err)
	}
	getChunkSuccess.Add(1)
	glog.V(3).Infof("Fetched %x in %v", sha256sum, time.Since(start))
	if zb != nil {
		glog.V(5).Infof("Used the zbyte! (%q + %q size: %d of %d)", zb, chunk[0:7], len(chunk), file.Size)
		chunk = append(zb, chunk...)
	}
	return chunk, nil
}

func getZerobyte(file *gdrive.File) ([]byte, error) {
	if file.Properties == nil {
		return nil, errors.New("no Properties, so no zerobyte")
	}
	zbhex, ok := file.Properties["zb"]
	if !ok {
		return nil, errors.New("properties have no zerobyte")
	}
	zb, err := hex.DecodeString(zbhex)
	if err != nil {
		return nil, errors.New("could not decode zbyte")
	}
	return zb, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256sum, content []byte, f *shade.File) error {
	if f == nil {
		return errors.New("google.PutChunk requires an associated File{} object")
	}
	putChunkReq.Add(1)
	df := &gdrive.File{
		Name:          hex.EncodeToString(sha256sum),
		AppProperties: map[string]string{"shadeType": "chunk"},
		Properties:    map[string]string{"zb": hex.EncodeToString(content[0:1])},
	}
	if s.config.ChunkParentID != "" {
		df.Parents = []string{s.config.ChunkParentID}
	}

	// Avoid the Google Drive API dividing the upload into smaller chunks.
	opts := []googleapi.MediaOption{googleapi.ChunkSize(len(content))}
	// If there is more than one chunk set the content-type explicitly for the
	// upload.  Even if it is unencrypted and happens to look like a valid
	// mime-type, it is not a complete file.  It would be preferrable
	// for Google not try to display it to the user in the web UI.
	if len(f.Chunks) > 1 {
		opts = append(opts, googleapi.ContentType("application/octet-stream"))
	}

	ctx := context.TODO() // TODO(cfunkhouser): Get a meaningful context here.
	br := bytes.NewReader(content)
	if _, err := s.service.Files.Create(df).SupportsTeamDrives(true).Context(ctx).Media(br, opts...).Do(); err != nil {
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
