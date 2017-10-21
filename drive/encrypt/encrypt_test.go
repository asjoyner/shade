package encrypt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"testing"

	"github.com/asjoyner/shade/drive"
	_ "github.com/asjoyner/shade/drive/memory"
)

func TestFileRoundTrip(t *testing.T) {
	tc, err := testClient()
	if err != nil {
		t.Fatalf("TestClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, tc, 2)
}

func TestChunkRoundTrip(t *testing.T) {
	tc, err := testClient()
	if err != nil {
		t.Fatalf("TestClient() for test config failed: %s", err)
	}
	drive.TestChunkRoundTrip(t, tc, 100)
}

func TestParallelRoundTrip(t *testing.T) {
	tc, err := testClient()
	if err != nil {
		t.Fatalf("TestClient() for test config failed: %s", err)
	}
	drive.TestParallelRoundTrip(t, tc, 100)
}

func testClient() (drive.Client, error) {
	privkey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return NewClient(drive.Config{
		Provider:      "encrypt",
		RsaPrivateKey: x509.MarshalPKCS1PrivateKey(privkey),
		Children:      []drive.Config{{Provider: "memory", Write: true}},
	})
}
