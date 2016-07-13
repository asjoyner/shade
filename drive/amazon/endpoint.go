package amazon

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/cenk/backoff"
)

const (
	endpointURL string = "https://drive.amazonaws.com/drive/v1/account/endpoint"
)

// Endpoint provides the URLs the drive service should talk to.
//
// It provides threadsafe methods to get the Content and Metadata URLs, to
// lookup new ones, and to keep them persistently up to date.
type Endpoint struct {
	client      *http.Client
	contentURL  string
	metadataURL string
	sync.RWMutex
}

// NewEndpoint returns an initialized Endpoint, or an error.
func NewEndpoint(c *http.Client) (*Endpoint, error) {
	ep := &Endpoint{client: c}
	if err := ep.GetEndpoint(); err != nil {
		return nil, err
	}
	go ep.RefreshEndpoint()
	return ep, nil
}

type endpointResponse struct {
	ContentURL     string // eg. https://content-na.drive.amazonaws.com/cdproxy/
	MetadataURL    string // eg. https://cdws.us-east-1.amazonaws.com/drive/v1/"
	CustomerExists bool
}

// GetEndpoint requests the URL for this user to send queries to.
func (ep *Endpoint) GetEndpoint() error {
	resp, err := ep.client.Get(endpointURL)
	if err != nil {
		return fmt.Errorf("Get(endpointURL): %s", err)
	}
	defer resp.Body.Close()

	// unpack this JSON response
	endpointJSON, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var epResp endpointResponse
	if err := json.Unmarshal(endpointJSON, &epResp); err != nil {
		return fmt.Errorf("json unmarshal error: %s", err)
	}

	// update the URLs
	ep.Lock()
	defer ep.Unlock()
	ep.contentURL = epResp.ContentURL
	ep.metadataURL = epResp.MetadataURL
	return nil
}

// refreshEndpoint periodically calls GetEndpoint
// This needs to be run every 3-5 days, per:
// https://developer.amazon.com/public/apis/experience/cloud-drive/content/account
//
// TODO(asjoyner): cache this, and save 1 RPC for every invocation of throw
func (ep *Endpoint) RefreshEndpoint() {
	for {
		if err := backoff.Retry(ep.GetEndpoint, backoff.NewExponentialBackOff()); err != nil {
			// Failed for 15 minutes, lets sleep for a couple hours and try again.
			time.Sleep(2 * time.Hour)
		} else {
			// Success!  Hibernation time...
			time.Sleep(72 * time.Hour)
		}
	}
}

func (ep *Endpoint) MetadataURL() string {
	ep.RLock()
	defer ep.RUnlock()
	return ep.metadataURL
}

func (ep *Endpoint) ContentURL() string {
	ep.RLock()
	defer ep.RUnlock()
	return ep.contentURL
}
