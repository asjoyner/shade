// zerobyte iterates all the shade files, reads their first byte, and adds it
// as a Property of the file.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	gdrive "google.golang.org/api/drive/v3"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive/google"
	"github.com/golang/glog"
)

var (
	defaultConfig = path.Join(shade.ConfigDir(), "google-only-config.json")

	// flags
	configPath = flag.String("config", defaultConfig, "shade config file containing only one entry for google")
	filename   = flag.String("filename", "*", "restrict to this filename")
	numWorkers = flag.Int("numWorkers", 10, "The number of files to fix in parallel.")
	mimeType   = flag.String("mimetype", "*", "restrict to this mimetype")
	refresh    = flag.Bool("refresh", false, "check and (if necessary, correct) values of zb")
	shadeType  = flag.Bool("shadeType", true, "work on files files which were created by shade?")
)

func main() {
	flag.Parse()
	// read in the config
	cfg, err := config.Read(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not initialize clients: %s\n", err)
		os.Exit(1)
	}

	client := google.GetOAuthClient(cfg)
	service, err := gdrive.New(client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to retrieve Google Drive Client: %s\n", err)
		os.Exit(2)
	}

	ctx := context.Background()
	found := make(chan *gdrive.File)
	var wg sync.WaitGroup
	wg.Add(*numWorkers)

	// prepare worker goroutines
	for w := 1; w <= *numWorkers; w++ {
		go func() {
			for f := range found {
				if err := fixZeroByte(ctx, service, f); err != nil {
					glog.Info(err)
				}
			}
			wg.Done()
		}()
	}

	// lookup files and pass to goroutines
	findFiles(ctx, service, found)
	wg.Wait()
	glog.Info("Done!")
}

// findFiles iteratively downlaods the list of File objects from Google Drive
// that need to be updated, and writes them to 'found'
func findFiles(ctx context.Context, service *gdrive.Service, found chan *gdrive.File) {
	var numFiles int
	p := pages{numFiles: numFiles, found: found}

	q := fmt.Sprintf("name contains '%s' and mimeType contains '%s' and ", *filename, *mimeType)
	if !*shadeType {
		q += "not "
	}
	q += "(appProperties has { key='shadeType' and value='file' } or appProperties has { key='shadeType' and value='chunk' })"
	/*
		// it's too bad you can not use a query like this one:
		if !*refresh {
			q = fmt.Sprintf("%s and not properties has { key='zb' }", q)
		}
	*/
	glog.V(2).Infof("files.list query: %s", q)
	req := service.Files.List().IncludeTeamDriveItems(true).SupportsTeamDrives(true)
	req = req.Context(ctx).Q(q).Fields("files(id, name, properties, mimeType), nextPageToken")
	req = req.PageSize(1000) //.Corpora("user,allTeamDrives")
	err := req.Pages(ctx, p.handlePage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't retrieve file list: %s\n", err)
		os.Exit(3)
	}
	close(found)
}

// pages holds the state about pages as they are processed
type pages struct {
	numFiles int
	found    chan *gdrive.File
}

// handlePage processes each page of files returned by the files.list query.
// It will be called one or more times as Files.List()...Pages() iterates over
// the paginated responses from Google Drive.
func (p *pages) handlePage(r *gdrive.FileList) error {
	for _, f := range r.Files {
		if f.MimeType == "application/vnd.google-apps.folder" {
			continue
		}
		p.numFiles++
		if f.Properties != nil {
			if zb, ok := f.Properties["zb"]; ok {
				if !*refresh {
					glog.V(2).Infof("Skipping file %s (%s) with zb: %s", f.Name, f.Id, zb)
					continue
				}
			}
		}
		glog.V(3).Infof("requesting processing of file: %s (%s)", f.Name, f.Id)
		p.found <- f
	}
	glog.Infof("Processed %d files.", p.numFiles)
	return nil
}

func fixZeroByte(ctx context.Context, service *gdrive.Service, f *gdrive.File) error {
	// Fetch the zerobyte
	req := service.Files.Get(f.Id).SupportsTeamDrives(true)
	req.Header().Add("Range", "bytes=0-0")
	resp, err := req.Download()
	if err != nil {
		if strings.Contains(err.Error(), "downloadQuotaExceeded") {
			err = errors.New("quota")
		}
		return fmt.Errorf("couldn't download zerobyte of %s (%s): %s", f.Name, f.Id, err)
	}
	defer resp.Body.Close()

	halfMagic, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("couldn't read file %s (%s): %v", f.Name, f.Id, err)
	}
	glog.V(4).Infof("The first byte is: %x\n", halfMagic)

	// Update file with zb Property
	u := &gdrive.File{
		Properties: map[string]string{"zb": hex.EncodeToString(halfMagic)},
	}
	uf, err := service.Files.Update(f.Id, u).SupportsTeamDrives(true).Context(ctx).Fields("id, name, properties").Do()
	if err != nil {
		return fmt.Errorf("couldn't update %s (%s): %s", uf.Name, uf.Id, err)
	}

	glog.Infof("Added zb=%q to %s", uf.Properties["zb"], uf.Name)
	return nil
}
