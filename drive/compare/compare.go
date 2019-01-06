package compare

import "github.com/asjoyner/shade/drive"

// Delta describes the extra file and chunk sums known to one client, which are
// not known to the other.
type Delta struct {
	Files  [][]byte
	Chunks [][]byte
}

// Equal returns true if client a and b return the same sums via ListFiles and
// their ChunkLister interface.
//
// See the disclaimers for GetDelta, which this is a thin veneer on top of.
func Equal(a, b drive.Client) (bool, error) {
	aDelta, bDelta, err := GetDelta(a, b)
	if err != nil {
		return false, err
	}
	if aDelta.Files == nil &&
		aDelta.Chunks == nil &&
		bDelta.Files == nil &&
		bDelta.Chunks == nil {
		return true, nil
	}
	return false, nil
}

// GetDelta iterates the files and chunks in two drive clients, and returns two
// Delta structs, containing slices of files and chunks.  The return values
// are:
//   - files and chunks that a has that b does not
//   - files and chunks that b has that a does not
//   - an error, if one was encountered
//
// Nb: This is intended only for use on relatively small Shade repositorites,
// primarily for testing.  Depending on the client, it may consume a lot of ram
// and take a very long time to run.
//
// Nb: it does not validate the clients contain the same actual bytes, it
// trusts the sums reported by ListFiles and the ChunkLister interface.
func GetDelta(a, b drive.Client) (Delta, Delta, error) {
	var aDelta, bDelta Delta
	af, err := a.ListFiles()
	if err != nil {
		return Delta{}, Delta{}, err
	}
	aFiles := make(map[string]struct{})
	for _, sum := range af {
		aFiles[string(sum)] = struct{}{}
	}
	bf, err := b.ListFiles()
	if err != nil {
		return Delta{}, Delta{}, err
	}
	bFiles := make(map[string]struct{})
	for _, sum := range bf {
		bFiles[string(sum)] = struct{}{}
	}
	for f := range aFiles {
		if _, ok := bFiles[f]; !ok {
			aDelta.Files = append(aDelta.Files, []byte(f))
		}
	}
	for f := range bFiles {
		if _, ok := aFiles[f]; !ok {
			bDelta.Files = append(bDelta.Files, []byte(f))
		}
	}
	ac, err := AllChunkSums(a.NewChunkLister())
	if err != nil {
		return Delta{}, Delta{}, err
	}
	aChunks := make(map[string]struct{})
	for _, sum := range ac {
		aChunks[string(sum)] = struct{}{}
	}

	bc, err := AllChunkSums(b.NewChunkLister())
	if err != nil {
		return Delta{}, Delta{}, err
	}
	bChunks := make(map[string]struct{})
	for _, sum := range bc {
		bChunks[string(sum)] = struct{}{}
	}
	for f := range aChunks {
		if _, ok := bChunks[f]; !ok {
			aDelta.Chunks = append(aDelta.Chunks, []byte(f))
		}
	}
	for f := range bChunks {
		if _, ok := aChunks[f]; !ok {
			bDelta.Chunks = append(bDelta.Chunks, []byte(f))
		}
	}
	return aDelta, bDelta, nil
}

// AllChunkSums iterates lister.  It gathers and returns all of the sums.
//
// Nb: It is intended only for use on relatively small Shade repositorites.
// Depending on the client, it may consume a lot of ram and take a very
// long time to run.
func AllChunkSums(lister drive.ChunkLister) ([][]byte, error) {
	var chunkSums [][]byte
	for lister.Next() {
		chunkSums = append(chunkSums, lister.Sha256())
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	return chunkSums, nil
}
