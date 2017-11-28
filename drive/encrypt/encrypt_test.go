package encrypt

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
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
	return NewClient(drive.Config{
		Provider:      "encrypt",
		RsaPrivateKey: x509.MarshalPKCS1PrivateKey(privkey),
		Children:      []drive.Config{{Provider: "memory", Write: true}},
	})
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
