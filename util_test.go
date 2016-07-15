package shade

import "testing"

func TestConfigDir(t *testing.T) {
	origGOOS := goos
	origEnvFunc := envFunc
	defer func() {
		goos = origGOOS
		envFunc = origEnvFunc
	}()
	envFunc = func(_ string) string {
		return "/foo/bar"
	}

	for _, tt := range []struct {
		goos string
		want string
	}{
		{"linux", "/foo/bar/.shade"},
		{"freebsd", "/foo/bar/.shade"},
		{"darwin", "/foo/bar/Library/Application Support/shade"},
		{"bogus", "."},
	} {
		goos = tt.goos
		if got := ConfigDir(); got != tt.want {
			t.Errorf("Wanted %q from ConfigDir() for GOOS = %q, but got %q", tt.want, tt.goos, got)
		}
	}
}
