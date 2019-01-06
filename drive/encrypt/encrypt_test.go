package encrypt

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	_ "github.com/asjoyner/shade/drive/memory"
)

func TestFileRoundTrip(t *testing.T) {
	tc, err := testClient()
	if err != nil {
		t.Fatalf("TestClient() for test config failed: %s", err)
	}
	drive.TestFileRoundTrip(t, tc, 100)
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
	derBytes := x509.MarshalPKCS1PrivateKey(privkey)
	b := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: derBytes}
	pemPrivKey := string(pem.EncodeToMemory(b))
	return NewClient(drive.Config{
		Provider:      "encrypt",
		RsaPrivateKey: pemPrivKey,
		Children:      []drive.Config{{Provider: "memory", Write: true}},
	})
}

func TestNewClient(t *testing.T) {
	configs := []drive.Config{
		drive.Config{
			Provider:      "encrypt",
			RsaPrivateKey: "-----BEGIN RSA PRIVATE KEY-----\nMDkCAQACCADc5XG/z8hNAgMBAAECB0hGXla5p8ECBA+wdzECBA4UU90CBA4/9MEC\nBALlFRUCBAKTsts=\n-----END RSA PRIVATE KEY-----",
			Children:      []drive.Config{{Provider: "memory", Write: true}},
		},
		drive.Config{
			Provider:     "encrypt",
			RsaPublicKey: "-----BEGIN PUBLIC KEY-----\nMCMwDQYJKoZIhvcNAQEBBQADEgAwDwIIANzlcb/PyE0CAwEAAQ==\n-----END PUBLIC KEY-----",
			Children:     []drive.Config{{Provider: "memory", Write: true}},
		},
	}
	for _, c := range configs {
		_, err := NewClient(c)
		if err != nil {
			t.Errorf("Failed to initialize client : %s", err)
		}
	}
}

// This independently tests the basic primitives on which the Drive
// implementation is built.
func TestEncryptDecrypt(t *testing.T) {
	plaintext := []byte("abc123")
	key := shade.NewSymmetricKey()
	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("encrypting: %s", err)
	}
	response, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatalf("decrypting: %s", err)
	}
	if !bytes.Equal(plaintext, response) {
		t.Fatalf("want: %q, got: %q", plaintext, response)
	}
}

func TestFileRelease(t *testing.T) {
	tc, err := testClient()
	if err != nil {
		t.Fatalf("TestClient() for test config failed: %s", err)
	}
	// validate is set to false, because drive/test.go can't easily determine
	// encrypted chunk sums
	drive.TestRelease(t, tc, false)
}
