// +build remote

package google

import (
	"flag"
	"fmt"
	"log"
	"testing"

	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"
)

var (
	configPath = flag.String("googleTestConfig", "testdata/config.json", "Path to the config to do remote integration testing.")
)

func newTestClient() (drive.Client, err) {
	cfg, err := config.Read(*configPath)
	if err != nil {
		return nil, fmt.Errorf("could not read remote test config from %s: %s", *configPath, err)
	}
	client, err := drive.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not initialize test client: %s\n", err)
	}
	return client, nil
}

func TestFileRoundTrip(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		log.Fatalf(err)
	}
	drive.TestFileRoundTrip(t, client, 10)
}

func TestChunkRoundTrip(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		log.Fatalf(err)
	}
	drive.TestChunkRoundTrip(t, client, 10)
}

func TestParallelRoundTrip(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		log.Fatalf(err)
	}
	drive.TestParallelRoundTrip(t, client, 10)
}

func TestChunkLister(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		log.Fatalf(err)
	}
	cfg, err := config.Read(*configPath)
	drive.TestChunkLister(t, client, 10)
}
