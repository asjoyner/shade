package amazon

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
)

var (
	listFileReq = expvar.NewInt("amazonListFilesReq")
	getFileReq  = expvar.NewInt("amazonGetFileReq")
	putFileReq  = expvar.NewInt("amazonPutFileReq")
	getChunkReq = expvar.NewInt("amazonGetChunkReq")
	putChunkReq = expvar.NewInt("amazonPutChunkReq")
)

func init() {
	drive.RegisterProvider("amazon", NewClient)
}

type getFilesResponse struct {
	Count     int64
	NextToken string
	Data      []fileMetadata
}

type fileMetadata struct {
	ID           string
	Name         string
	ModifiedDate string
	CreatedDate  string
	Labels       []string
	Size         int64
	//Description  string
	//Status       string
	//ContentType  string
}

// NewClient returns an initialized Drive drive.Client object.
func NewClient(c drive.Config) (drive.Client, error) {
	client, err := getOAuthClient(c)
	if err != nil {
		return nil, err
	}
	ep, err := NewEndpoint(client) // endpoint.go
	if err != nil {
		return nil, err
	}
	return &Drive{
		client: client,
		ep:     ep,
		config: c,
		files:  make(map[string]string),
	}, nil
}

// Drive is a representation of the Amazon Cloud storage system.
type Drive struct {
	client *http.Client
	ep     *Endpoint
	config drive.Config
	// files maps from the string([]byte) representation of the file's SHA2 to
	// the corresponding fileID in Drive
	files map[string]string
	fm    sync.RWMutex // protects files
}

// ListFiles retrieves all of the File objects known to the client, and returns
// the corresponding sha256sum of the file object.  Those may be passed to
// GetFile() to retrieve the corresponding shade.File.
func (s *Drive) ListFiles() ([][]byte, error) {
	listFileReq.Add(1)
	// a list mapping the ID(s) of the shade.File(s) in Drive to sha256sum
	filters := "kind:FILE AND labels:shadeFile"
	if s.config.FileParentID != "" {
		filters += " AND parents:s.config.FileParentID"
	}

	v := url.Values{}
	v.Set("filters", filters)
	// TODO(asjoyner): sort modifiedDate:DESC and stop on last seen time?
	//v.Set("sort", `["modifiedDate DESC"]`)

	var nextToken string
	for {
		if nextToken != "" {
			v.Set("startToken", nextToken)
		}
		gfResp, err := s.getMetadata(v)
		if err != nil {
			return nil, err
		}

		s.fm.Lock()
		for _, f := range gfResp.Data {
			b, err := hex.DecodeString(f.Name)
			if err != nil {
				log.Printf("Shade file %q with invalid hex in filename: %s\n", f.Name, err)
			}
			s.files[string(b)] = f.ID
		}
		s.fm.Unlock()
		if gfResp.NextToken == "" {
			break
		}
		nextToken = gfResp.NextToken
	}

	s.fm.RLock()
	defer s.fm.RUnlock()
	resp := make([][]byte, 0, len(s.files))
	for sha256sum := range s.files {
		resp = append(resp, []byte(sha256sum))
	}
	return resp, nil
}

// GetFile retrieves a file by sha256sum, as returned by ListFiles().
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) GetFile(sha256sum []byte) ([]byte, error) {
	getFileReq.Add(1)
	return s.GetChunk(sha256sum, nil)
}

// PutFile writes the manifest describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *Drive) PutFile(sha256sum, contents []byte) error {
	putFileReq.Add(1)
	filename := hex.EncodeToString(sha256sum)
	metadata := map[string]interface{}{
		"kind":   "FILE",
		"name":   filename,
		"labels": []string{"shadeFile"},
		// "properties": ... shade version id?
	}
	if s.config.FileParentID != "" {
		metadata["parents"] = s.config.FileParentID
	}
	if err := s.uploadFile(filename, contents, metadata); err != nil {
		return err
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum.
// It first gets the ID of the chunk with the named sha256sum, possibly from a
// cache.  If then fetches the contents of the chunk from Drive.
//
// The cache is especially helpful for shade.File objects, which are
// efficiently looked up on each call of ListFiles.
func (s *Drive) GetChunk(sha256sum []byte, f *shade.File) ([]byte, error) {
	getChunkReq.Add(1)
	s.fm.RLock()
	fileID, ok := s.files[string(sha256sum)]
	s.fm.RUnlock()

	if !ok { // we have to lookup this fileID
		filters := fmt.Sprintf("kind:FILE AND labels:shadeChunk AND name:%x", sha256sum)
		v := url.Values{}
		v.Set("filters", filters)

		gfResp, err := s.getMetadata(v)
		if err != nil {
			return nil, err
		}

		if len(gfResp.Data) > 1 {
			return nil, fmt.Errorf("More than one file with SHA sum: %x", sha256sum)
		}
		fileID = gfResp.Data[0].ID
	}

	// Get the contents of the fileIDs.
	c, err := s.getFileContents(fileID)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *Drive) PutChunk(sha256sum []byte, chunk []byte, f *shade.File) error {
	putChunkReq.Add(1)
	s.fm.RLock()
	_, ok := s.files[string(sha256sum)]
	s.fm.RUnlock()
	if ok {
		return nil // we know this chunk already exists
	}

	filename := hex.EncodeToString(sha256sum)
	metadata := map[string]interface{}{
		"kind":   "FILE",
		"name":   filename,
		"labels": []string{"shadeChunk"},
		// "properties": ... shade version id?  manifest back reference?
	}
	if s.config.ChunkParentID != "" {
		metadata["parents"] = s.config.ChunkParentID
	}
	if err := s.uploadFile(filename, chunk, metadata); err != nil {
		return err
	}
	return nil
}

// GetConfig returns the Drive's associated Config object.
func (s *Drive) GetConfig() drive.Config {
	return s.config
}

// Local returns whether the storage is local to this machine.
func (s *Drive) Local() bool { return false }

// Persistent returns whether the storage is persistent across task restarts.
func (s *Drive) Persistent() bool { return true }

// uploadFile pushes the file with the associated metadata describing it
func (s *Drive) uploadFile(filename string, chunk []byte, metadata interface{}) error {
	body, ctype, err := mimeEncode(filename, chunk, metadata)
	if err != nil {
		return err
	}
	url := s.ep.ContentURL() + "nodes"
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", ctype)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 409 {
		// "409 Conflict" indicates the file already exists at this path with this
		// name.  Thus, we do not consider this an error.
		return nil
	} else if resp.StatusCode != 201 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		return fmt.Errorf("upload failed: %s: %s", resp.Status, buf.String())
	}
	return nil
}

// mimeEncode creates multipart MIME body text for a file upload
//
// filename and data describe the file to be uploaded, metadata is a set of
// key/value pairs to be associated with the file.
//
// It returns the body as an io.Writer and the matching content-type header, or
// an error.  content-type has to be calculated by the multipart.Writer object
// because it contains the magic string "border" which separates mime parts.
//
// For format details, see the sample request here:
// https://developer.amazon.com/public/apis/experience/cloud-drive/content/nodes
func mimeEncode(filename string, data []byte, metadata interface{}) (io.Reader, string, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Order matters: metadata must come before content in the MIME content
	m, err := json.Marshal(metadata)
	if err != nil {
		return nil, "", err
	}
	_ = writer.WriteField("metadata", string(m))

	part, err := writer.CreateFormFile("content", filename)
	if err != nil {
		return nil, "", err
	}
	if _, err := part.Write(data); err != nil {
		return nil, "", err
	}

	err = writer.Close()
	if err != nil {
		return nil, "", err
	}
	ctype := writer.FormDataContentType()
	return body, ctype, err
}

// getMetadata retrieves the metadata of at most 200 shade.File(s) stored
// in Drive, unmarshals the JSON and returns them.  Use NextToken to
// request the next set of responses.
func (s *Drive) getMetadata(v url.Values) (getFilesResponse, error) {
	// URL from docs here:
	// https://developer.amazon.com/public/apis/experience/cloud-drive/content/nodes
	req := fmt.Sprintf("%s/nodes?%s", s.ep.MetadataURL(), v.Encode())
	resp, err := s.client.Get(req)
	if err != nil {
		return getFilesResponse{}, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if resp.StatusCode != 200 {
		return getFilesResponse{}, fmt.Errorf("%s: %s", resp.Status, buf.String())
	}

	// Unmarshal the Amazon metadata about our file object
	var gfResp getFilesResponse
	if err := json.Unmarshal(buf.Bytes(), &gfResp); err != nil {
		return getFilesResponse{}, fmt.Errorf("json unmarshal error: %s", err)
	}
	return gfResp, nil
}

// getFileContents downloads the contents of a given file ID from Drive
// Documentation on the download URL and parameters are here:
// https://developer.amazon.com/public/apis/experience/cloud-drive/content/nodes
func (s *Drive) getFileContents(id string) ([]byte, error) {
	url := fmt.Sprintf("%snodes/%s/content", s.ep.ContentURL(), id)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", resp.Status, buf.String())
	}
	return buf.Bytes(), nil
}
