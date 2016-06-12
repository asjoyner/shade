package amazon

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"github.com/asjoyner/shade/drive"
)

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

// GetFiles retrieves all of the File objects known to the client.
// The responses are marshalled JSON, which may be encrypted.
func (s *AmazonCloudDrive) GetFiles() ([]byte, error) {
	return nil, nil
}

// PutFile writes the manifest describing a new file.
// f should be marshalled JSON, and may be encrypted.
func (s *AmazonCloudDrive) PutFile(f []byte) error {
	a := sha256.Sum256(f)
	filename := fmt.Sprintf("%x", a[:])
	metadata := map[string]interface{}{
		"kind":   "FILE",
		"name":   filename,
		"labels": []string{"shadeFile"},
		// "properties": ... shade version id?
	}
	if s.config.FileParentID != "" {
		metadata["parents"] = s.config.FileParentID
	}
	if err := s.uploadFile(filename, f, metadata); err != nil {
		return err
	}
	return nil
}

// GetChunk retrieves a chunk with a given SHA-256 sum
func (s *AmazonCloudDrive) GetChunk(sha256 []byte) ([]byte, error) {
	return nil, nil
}

// PutChunk writes a chunk and returns its SHA-256 sum
func (s *AmazonCloudDrive) PutChunk(sha256 []byte, chunk []byte) error {
	filename := fmt.Sprintf("%x", sha256)
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
