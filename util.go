package shade

import (
	"log"
	"os"
	"path"
	"runtime"
)

var (
	// These are mockable for testing.
	envFunc = os.Getenv
	goos    = runtime.GOOS
)

// ConfigDir identifies the correct path to store persistent configuration data
// on various operating systems.
func ConfigDir() string {
	dir := "."
	switch goos {
	case "darwin":
		dir = path.Join(envFunc("HOME"), "Library", "Application Support", "shade")
	case "linux", "freebsd":
		dir = path.Join(envFunc("HOME"), ".shade")
	default:
		log.Printf("TODO: ConfigDir on GOOS %q", goos)
	}
	return dir
}
