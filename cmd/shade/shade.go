// shade presents a fuse filesystem interface.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"time"

	"bazil.org/fuse"

	"github.com/asjoyner/shade/cache"
	"github.com/asjoyner/shade/config"
	"github.com/asjoyner/shade/drive"

	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/localdrive"
	_ "github.com/asjoyner/shade/drive/memory"
)

var (
	readOnly   = flag.Bool("readonly", false, "Mount the filesystem read only.")
	allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")
)

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	// read in the config
	clients, err := config.Clients()
	if err != nil {
		log.Fatalf("could not initialize clients: %s\n", err)
	}

	// Setup fuse FS
	conn, err := mountFuse(flag.Arg(0))
	if err != nil {
		log.Fatalf("failed to mount: %s", err)
	}
	fmt.Printf("Mounting Shade FuseFS at %s...\n", flag.Arg(0))

	if err := serviceFuse(conn, clients); err != nil {
		log.Fatalf("failed to service mount: %s", err)
	}

	return
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <mountpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func mountFuse(mountPoint string) (*fuse.Conn, error) {
	if err := sanityCheck(mountPoint); err != nil {
		return nil, fmt.Errorf("sanityCheck failed: %s\n", err)
	}

	options := []fuse.MountOption{
		fuse.FSName("Shade"),
		//fuse.Subtype(""),
		//fuse.VolumeName(<iterate clients?>),
	}

	if *allowOther {
		options = append(options, fuse.AllowOther())
	}
	if *readOnly {
		options = append(options, fuse.ReadOnly())
	}
	options = append(options, fuse.NoAppleDouble())
	c, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		fmt.Println("Is the mount point busy?")
		return nil, err
	}

	// Trap control-c (sig INT) and unmount
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		for _ = range sig {
			if err := fuse.Unmount(mountPoint); err != nil {
				log.Printf("fuse.Unmount failed: %v", err)
			}
		}
	}()

	return c, nil
}

// serviceFuse responds to incoming fuse requests until the FS is unmounted.
func serviceFuse(conn *fuse.Conn, clients []drive.Client) error {
	uid, gid, err := uidAndGid()
	if err != nil {
		return fmt.Errorf("unable to get UID/GID of current user: %v", err)
	}
	refresh := time.NewTicker(5 * time.Minute)
	r, err := cache.New(clients, refresh)
	if err != nil {
		return err
	}
	sc := NewFuseServer(r, uid, gid, conn)
	err = sc.Serve()
	if err != nil {
		return fmt.Errorf("fuse server initialization failed: %s", err)
	}

	// check if the mount process has an error to report
	<-conn.Ready
	if err := conn.MountError; err != nil {
		return err
	}
	return nil
}

// uidAndGid returns those values, or err
func uidAndGid() (uint32, uint32, error) {
	userCurrent, err := user.Current()
	if err != nil {
		return 0, 0, err
	}
	uidInt, err := strconv.Atoi(userCurrent.Uid)
	if err != nil {
		return 0, 0, err
	}
	uid := uint32(uidInt)
	gidInt, err := strconv.Atoi(userCurrent.Gid)
	if err != nil {
		return 0, 0, err
	}
	gid := uint32(gidInt)

	return uid, gid, nil
}

func sanityCheck(mountPoint string) error {
	fileInfo, err := os.Stat(mountPoint)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(mountPoint, 0777); err != nil {
			return fmt.Errorf("mountpoint does not exist, could not create it.")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("error stat()ing mountpoint: %s", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the mountpoint is not a directory")
	}
	return nil
}
