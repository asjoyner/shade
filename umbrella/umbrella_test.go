package umbrella

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/compare"
	"github.com/asjoyner/shade/drive/encrypt"
	"github.com/asjoyner/shade/drive/memory"
)

const chunkSize uint64 = 100 * 256

func newMemoryClient(t *testing.T) drive.Client {
	mc, err := memory.NewClient(drive.Config{Provider: "memory"})
	if err != nil {
		t.Fatalf("could not initilize test client: %s", err)
	}
	return mc
}

func TestFetchingFiles(t *testing.T) {
	mc := newMemoryClient(t)

	// Make a new file with 1 chunk
	file := shade.NewFile("testfile")

	// Put the original copy of the file in the client
	putFile(t, mc, *file)

	// Push a version of the file with a newer mtime
	file.ModifiedTime = file.ModifiedTime.Add(1 * time.Minute)
	putFile(t, mc, *file)

	// Push an even newer version of the file that is deleted
	file.ModifiedTime = file.ModifiedTime.Add(1 * time.Minute)
	file.Deleted = true
	putFile(t, mc, *file)

	inUse, obsolete, err := FetchFiles(mc)
	if err != nil {
		t.Fatal(err)
	}

	if len(inUse) != 1 {
		t.Errorf("in use files unexpected, want: 1, got %d", len(inUse))
	}
	if len(obsolete) != 2 {
		t.Errorf("obsolete files unexpected, want: 2, got %d", len(obsolete))
	}

}

func TestRemovingOrphanedFiles(t *testing.T) {
	mc := newMemoryClient(t)
	expected := newMemoryClient(t)

	// Make a new file with 1 chunk
	file := shade.NewFile("testfile")
	sum, data := drive.RandChunk()
	chunk := shade.NewChunk()
	chunk.Index = 0
	chunk.Sha256 = []byte(sum)
	file.Chunks = append(file.Chunks, chunk)
	file.LastChunksize = int(chunkSize)

	if err := mc.PutChunk(sum, data, file); err != nil {
		t.Fatal(err)
	}
	if err := expected.PutChunk(sum, data, file); err != nil {
		t.Fatal(err)
	}

	// Put the original copy of the file in the client
	putFile(t, mc, *file)

	// Push a version of the file with a newer mtime
	file.ModifiedTime = file.ModifiedTime.Add(1 * time.Minute)
	putFile(t, mc, *file)

	// Push an even newer version of the file that is deleted
	file.ModifiedTime = file.ModifiedTime.Add(1 * time.Minute)
	file.Deleted = true
	putFile(t, mc, *file)
	putFile(t, expected, *file)

	err := Cleanup(mc)
	if err == nil {
		t.Errorf("want: 'more files are obsolete', got: nil")
	}
	if !strings.Contains(err.Error(), "more files are obsolete") {
		t.Errorf("want: 'more files are obsolete', got: %s", err)
	}

	// Push another file (similar content) to satisfy the safety threshold
	file.Filename = "testFile2"
	putFile(t, mc, *file)
	putFile(t, expected, *file)
	if err := Cleanup(mc); err != nil {
		t.Error(err)
	}

	// assert the actual class types to be able to check the internals
	if err := expected.(*memory.Drive).Equal(mc.(*memory.Drive)); err != nil {
		t.Fatal(err)
	}
}

func TestRemovingOrphanedChunks(t *testing.T) {
	mc := newMemoryClient(t)
	expected := newMemoryClient(t)

	// Make a new file with 1 chunk
	file := shade.NewFile("testfile")
	sum, data := drive.RandChunk()
	chunk := shade.NewChunk()
	chunk.Index = 0
	chunk.Sha256 = []byte(sum)
	file.Chunks = append(file.Chunks, chunk)
	file.LastChunksize = int(chunkSize)

	if err := mc.PutChunk(sum, data, file); err != nil {
		t.Fatal(err)
	}
	if err := expected.PutChunk(sum, data, file); err != nil {
		t.Fatal(err)
	}
	// Put some random unreferenced chunks, to be cleaned up
	for x := 0; x < 10; x++ {
		sum, data := drive.RandChunk()
		if err := mc.PutChunk(sum, data, file); err != nil {
			t.Fatal(err)
		}
	}

	// Put the original copy of the file in the client
	putFile(t, mc, *file)
	putFile(t, expected, *file)

	if err := Cleanup(mc); err != nil {
		t.Error(err)
	}

	// assert the actual class types to be able to check the internals
	if err := expected.(*memory.Drive).Equal(mc.(*memory.Drive)); err != nil {
		t.Fatal(err)
	}
}

func TestEncryptedClients(t *testing.T) {
	privkey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	derBytes := x509.MarshalPKCS1PrivateKey(privkey)
	b := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: derBytes}
	pemPrivKey := string(pem.EncodeToMemory(b))
	testClient, err := encrypt.NewClient(drive.Config{
		Provider:      "encrypt",
		RsaPrivateKey: pemPrivKey,
		Children:      []drive.Config{{Provider: "memory", Write: true}},
	})
	if err != nil {
		t.Fatalf("could not initilize test client: %s", err)
	}
	expected, err := encrypt.NewClient(drive.Config{
		Provider:      "encrypt",
		RsaPrivateKey: pemPrivKey,
		Children:      []drive.Config{{Provider: "memory", Write: true}},
	})
	if err != nil {
		t.Fatalf("could not initilize test client: %s", err)
	}
	unencrypted := newMemoryClient(t)

	numFiles := 4
	var keptFile *shade.File
	for f := 0; f <= numFiles; f++ {
		// Make a new file
		file := shade.NewFile("testfile")
		file.LastChunksize = int(chunkSize)
		// Add 4 chunks
		for x := 0; x < 4; x++ {
			sum, data := drive.RandChunk()
			chunk := shade.NewChunk()
			chunk.Index = x
			chunk.Sha256 = []byte(sum)
			file.Chunks = append(file.Chunks, chunk)
			if err := testClient.PutChunk(sum, data, file); err != nil {
				t.Fatal(err)
			}
			// Put only the chunks of the last file in the expected clients
			if f == numFiles {
				if err := expected.PutChunk(sum, data, file); err != nil {
					t.Fatal(err)
				}
				if err := unencrypted.PutChunk(sum, data, file); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Put only the last file in the client(s)
		if f == numFiles {
			putFile(t, testClient, *file)
			putFile(t, expected, *file)
			putFile(t, unencrypted, *file)
			keptFile = file
		}

	}

	if err := Cleanup(testClient); err != nil {
		t.Error(err)
	}

	// quick check to see if everything was okay
	ok, err := compare.Equal(expected, testClient)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("expected and test client are not equal!")
	}

	expectedExtras, testClientExtras, err := compare.GetDelta(expected, testClient)
	if err != nil {
		t.Fatal(err)
	}
	for _, sum := range testClientExtras.Files {
		t.Errorf("file was not cleaned up: %x", sum)
	}
	for _, sum := range testClientExtras.Chunks {
		t.Errorf("chunk was not cleaned up: %x", sum)
	}
	for _, sum := range expectedExtras.Files {
		t.Errorf("in-use file was deleted: %x", sum)
	}
	for _, sum := range expectedExtras.Chunks {
		t.Errorf("in-use chunk was deleted: %x", sum)
	}

	unencryptedExtras, testClientExtras, err := compare.GetDelta(unencrypted, testClient)
	// since we have the data handy, double-check that files coming back from the
	// encrypted client and unencrypted clients are ethe same.
	for _, sum := range testClientExtras.Files {
		t.Errorf("file was not cleaned up: %x", sum)
	}
	for _, sum := range unencryptedExtras.Files {
		t.Errorf("in-use file was deleted: %x", sum)
	}
	// check that each unencrypted chunk that wasn't the same actually matches
	// the encrypted chunk sum of the last file pushed
	for _, sum := range unencryptedExtras.Chunks {
		t.Logf("considering unencryptedExtra chunk: %x", sum)
		var found bool
		for _, esum := range testClientExtras.Chunks {
			s, err := encrypt.GetEncryptedSum(sum, keptFile)
			if err != nil {
				continue
			}
			if bytes.Equal(s, esum) {
				found = true
				t.Logf("matched unencryptedExtra chunk %x to %x", sum, esum)
			}
		}
		if !found {
			t.Errorf("in-use encrypted chunk sum was deleted: %x", sum)
		}
	}
}

// putFile wraps up the boilerplate to push a snapshot of a file into a given
// client.  It calls t.Fatalf if it encounters any unexpected errors.
func putFile(t *testing.T, client drive.Client, file shade.File) {
	jm, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	sum := shade.Sum(jm)
	t.Logf("Putting file with sum: %x", sum)
	if err := client.PutFile(sum, jm); err != nil {
		t.Fatal(err)
	}
}
