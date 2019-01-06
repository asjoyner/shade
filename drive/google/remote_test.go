// +build remote

package google

import (
	"flag"
	"testing"

	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"
)

var (
	configPath = flag.String("googleTestConfig", "testdata/config.json", "Path to the config to do remote integration testing.")
)

func newTestClient(t *testing.T) drive.Client {
	cfg, err := config.Read(*configPath)
	if err != nil {
		t.Fatalf("could not read remote test config from %s: %s", *configPath, err)
	}
	client, err := drive.NewClient(cfg)
	if err != nil {
		t.Fatalf("could not initialize test client: %s\n", err)
	}
	return client
}

func TestFileRoundTrip(t *testing.T) {
	client := newTestClient(t)
	drive.TestFileRoundTrip(t, client, 10)
}

func TestChunkRoundTrip(t *testing.T) {
	client := newTestClient(t)
	drive.TestChunkRoundTrip(t, client, 10)
}

func TestParallelRoundTrip(t *testing.T) {
	client := newTestClient(t)
	drive.TestParallelRoundTrip(t, client, 10)
}

func TestChunkLister(t *testing.T) {
	client := newTestClient(t)
	drive.TestChunkLister(t, client, 10)
}

func TestRelease(t *testing.T) {
	client := newTestClient(t)
	drive.TestRelease(t, client, true)
}
