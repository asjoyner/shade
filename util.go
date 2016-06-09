package shade

import (
	"fmt"
	"os"
	"path"
	"runtime"
)

// ConfigDir identifies the correct path to store persistent configuration data
// on various operating systems.
func ConfigDir() string {
	dir := "."
	switch runtime.GOOS {
	case "darwin":
		dir = path.Join(os.Getenv("HOME"), "Library", "Application Support", "shade")
	case "linux", "freebsd":
		dir = path.Join(os.Getenv("HOME"), ".shade")
	default:
		fmt.Printf("TODO: osUserCacheDir on GOOS %q", runtime.GOOS)
	}
	return dir
}
