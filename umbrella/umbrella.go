// Package umbrella provides utility functions to maintain your Shade
// repository, such as cleaning up orphaned files and chunks.
package umbrella

import (
	"flag"
	"fmt"
	"time"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"github.com/asjoyner/shade/drive/encrypt"
	"github.com/golang/glog"
)

var (
	maxFilesDelete  = flag.Int("maxFilesDelete", 100, "A safety limit: the maxmium number of files to delete per run.")
	maxChunksDelete = flag.Int("maxChunksDelete", 100, "A safety limit: the maxmium number of files to delete per run.")
)

// FoundFile groups files with their associated sums
type FoundFile struct {
	file *shade.File
	sum  []byte
}

// FetchFiles uses the provided client to fetch all of the known files and
// sorts them into those which are inUse and those which are obsolete.
func FetchFiles(client drive.Client) (inUse, obsolete []FoundFile, err error) {
	filesByPath := make(map[string]FoundFile)
	obsolete = make([]FoundFile, 0)
	// ListFiles to retrieve all file objects
	files, err := client.ListFiles()
	if err != nil {
		return nil, nil, fmt.Errorf("%q ListFiles(): %s", client.GetConfig().Provider, err)
	}
	glog.Infof("Found %d file(s) via %s", len(files), client.GetConfig().Provider)
	for _, sha256sum := range files {
		// fetch the file
		f, err := client.GetFile(sha256sum)
		if err != nil {
			glog.Warningf("Failed to fetch file %x: %s  (skipping)", sha256sum, err)
			continue
		}
		file := &shade.File{}
		if err := file.FromJSON(f); err != nil {
			glog.Warningf("Could not unmarshal file %x: %v", sha256sum, err)
			continue
		}
		existing, ok := filesByPath[file.Filename]
		if !ok {
			glog.V(2).Infof("found new file: %x", sha256sum)
			filesByPath[file.Filename] = FoundFile{file, sha256sum}
			continue
		}

		if existing.file.ModifiedTime.After(file.ModifiedTime) {
			glog.V(2).Infof("obsolete file: %x (%d < %d)", sha256sum, file.ModifiedTime.Unix(), existing.file.ModifiedTime.Unix())
			obsolete = append(obsolete, FoundFile{file, sha256sum})
			continue
		}
		filesByPath[file.Filename] = FoundFile{file, sha256sum}
		glog.V(2).Infof("obsolete file: %x (%d < %d)", existing.sum, file.ModifiedTime.Unix(), existing.file.ModifiedTime.Unix())
		obsolete = append(obsolete, FoundFile{existing.file, existing.sum})
	}
	inUse = make([]FoundFile, 0, len(filesByPath))
	for _, ff := range filesByPath {
		inUse = append(inUse, ff)
	}
	return
}

// Cleanup attempts to remove obsolete files and unused chunks from persistent
// storage clients.
func Cleanup(client drive.Client) error {
	inUse, obsolete, err := FetchFiles(client)
	if err != nil {
		return err
	}
	niu := len(inUse)
	no := len(obsolete)
	if niu < no {
		err := fmt.Errorf("more files are obsolete (%d) than remain (%d); aborting", no, niu)
		glog.Warning(err.Error())
		return err
	}
	if no > *maxFilesDelete {
		err := fmt.Errorf("num obsolete files (%d) over safety threshold (%d)", no, maxFilesDelete)
		glog.Warning(err.Error())
		return err
	}
	for _, ff := range obsolete {
		glog.Infof("Releasing obsolete file: %x", ff.sum)
		client.ReleaseFile(ff.sum)
	}

	// Build the map of all the chunksInUse
	chunksInUse := make(map[string]struct{})
	for _, ff := range inUse {
		for _, chunk := range ff.file.Chunks {
			chunksInUse[string(chunk.Sha256)] = struct{}{}
		}
		esums, err := encrypt.GetAllEncryptedSums(ff.file)
		if err != nil {
			summary := fmt.Sprintf("could not get encrypted sums for %s: %d", ff.file.Filename, len(esums))
			glog.Warningf("%s: %s", summary, err)
			return fmt.Errorf("%s: %s", summary, err)
		}
		glog.V(3).Infof("encrypted sums for %s: %d", ff.file.Filename, len(esums))
		for _, s := range esums {
			glog.V(7).Infof("valid encrypted sum: %x", s)
			chunksInUse[string(s)] = struct{}{}
		}
	}

	if err := cleanupUnusedFiles(client, chunksInUse); err != nil {
		return err
	}
	return nil
}

func cleanupUnusedFiles(client drive.Client, chunksInUse map[string]struct{}) error {
	var unusedChunks [][]byte
	lister := client.NewChunkLister()
	for lister.Next() {
		csum := lister.Sha256()
		if _, ok := chunksInUse[string(csum)]; !ok {
			unusedChunks = append(unusedChunks, csum)
			continue
		}
		glog.V(2).Infof("chunk is in use: %x", csum)
	}
	if err := lister.Err(); err != nil {
		return err
	}
	uc := len(unusedChunks)
	if uc >= *maxChunksDelete {
		err := fmt.Errorf("num unused chunks (%d) over safety threshold (%d)", uc, maxFilesDelete)
		glog.Warning(err.Error())
		return err
	}
	for _, csum := range unusedChunks {
		glog.V(2).Infof("removing unreferenced chunk: %x", csum)
		client.ReleaseChunk(csum)
	}
	return nil
}

// CleanupLoop calls Cleanup once per hour
func CleanupLoop(client drive.Client) {
	for {
		Cleanup(client)
		time.Sleep(1 * time.Hour)
	}
}
