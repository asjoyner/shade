package amazon

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/asjoyner/shade/drive"
)

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

func NewClient(c drive.Config) (drive.Client, error) {
	client, err := getOAuthClient(c)
	if err != nil {
		return nil, err
	}
	ep, err := NewEndpoint(client) // endpoint.go
	if err != nil {
		return nil, err
	}
	return &AmazonCloudDrive{client: client, ep: ep, config: c}, nil
}

type AmazonCloudDrive struct {
	client *http.Client
	ep     *Endpoint
	config drive.Config
}

// ListFiles retrieves all of the File objects known to the client.  The return
// maps from arbitrary unique keys to the sha256sum of the file object.  The
// keys may be passed to GetFile() to retrieve the corresponding shade.File.
func (s *AmazonCloudDrive) ListFiles() (map[string][]byte, error) {
	// a list mapping the ID(s) of the shade.File(s) in CloudDrive to sha256sum
	fileIDs := make(map[string][]byte)
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
		for _, f := range gfResp.Data {
			b, err := hex.DecodeString(f.Name)
			if err != nil {
				fmt.Printf("Shade file %q with invalid hex in filename: %s\n", f.Name, err)
			}
			fileIDs[f.ID] = b
		}
		if gfResp.NextToken == "" {
			break
		}
		nextToken = gfResp.NextToken
	}
	return fileIDs, nil
}

// GetFile retrieves the File described by the ID.
// The responses are marshalled JSON, which may be encrypted.
func (s *AmazonCloudDrive) GetFile(fileID string) ([]byte, error) {
	c, err := s.getFileContents(fileID)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// PutFile writes the manifest describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *AmazonCloudDrive) PutFile(sha256sum, contents []byte) error {
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

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *AmazonCloudDrive) GetChunk(sha256sum []byte) ([]byte, error) {
	// First, get the ID of the file with the named sha256 sum
	// TODO(asjoyner): keep a cache of this mapping to reduce read latency?
	filters := fmt.Sprintf("kind:FILE AND labels:shadeChunk AND name:%x", sha256sum)
	if s.config.ChunkParentID != "" {
		filters += " AND parents:s.config.ChunkParentID"
	}

	v := url.Values{}
	v.Set("filters", filters)

	gfResp, err := s.getMetadata(v)
	if err != nil {
		return nil, err
	}

	if len(gfResp.Data) > 1 {
		return nil, err
	}

	// Next, get the contents of the fileIDs.
	c, err := s.getFileContents(gfResp.Data[0].ID)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *AmazonCloudDrive) PutChunk(sha256sum []byte, chunk []byte) error {
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

func (s *AmazonCloudDrive) GetConfig() drive.Config {
	return s.config
}

func (s *AmazonCloudDrive) Local() bool { return false }

// uploadFile pushes the file with the associated metadata describing it
func (s *AmazonCloudDrive) uploadFile(filename string, chunk []byte, metadata interface{}) error {
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
	if resp.StatusCode != 201 {
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
// in CloudDrive, unmarshals the JSON and returns them.  Use NextToken to
// request the next set of responses.
func (s *AmazonCloudDrive) getMetadata(v url.Values) (getFilesResponse, error) {
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

// getFileContents downloads the contents of a given file ID from CloudDrive
// Documentation on the download URL and parameters are here:
// https://developer.amazon.com/public/apis/experience/cloud-drive/content/nodes
func (s *AmazonCloudDrive) getFileContents(id string) ([]byte, error) {
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
