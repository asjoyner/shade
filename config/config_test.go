package config

import (
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/asjoyner/shade/drive"

	// These are imported here to register them as drive client providers, and
	// are tested below.
	_ "github.com/asjoyner/shade/drive/amazon"
	_ "github.com/asjoyner/shade/drive/cache"
	_ "github.com/asjoyner/shade/drive/encrypt"
	_ "github.com/asjoyner/shade/drive/google"
	_ "github.com/asjoyner/shade/drive/local"
	_ "github.com/asjoyner/shade/drive/memory"
)

type testCase struct {
	name       string
	configPath string
	config     []byte
	want       drive.Config
	err        string
}

// initialize reads into config the contents of the file at configPath, if
// provided.
func (t *testCase) initialize() error {
	if t.configPath != "" {
		c, err := ioutil.ReadFile(t.configPath)
		if err != nil {
			return err
		}
		t.config = c
	}
	return nil
}

func TestParseConfig(t *testing.T) {
	for _, tc := range []testCase{
		{
			name:   "zero-byte config",
			config: []byte{},
			err:    "json unmarshal error",
		},
		{
			name:   "empty config",
			config: []byte("{}"),
			err:    "unsupported provider",
		},
		{
			name:       "bad provider",
			configPath: "testdata/bad-provider.config.json",
			err:        `unsupported provider in config: "thiswillneverexist"`,
		},
		{
			name:       "single amazon provider",
			configPath: "testdata/single-amazon-provider.config.json",
			want: drive.Config{
				Provider: "amazon",
				OAuth: drive.OAuthConfig{
					ClientID:     "12345",
					ClientSecret: "abcde",
					Scopes: []string{
						"clouddrive:read_other clouddrive:write",
					},
					TokenPath: "/dev/null",
				},
				FileParentID:  "1",
				ChunkParentID: "2",
				Write:         false,
			},
		},
		{
			name:       "single google provider",
			configPath: "testdata/single-google-provider.config.json",
			want: drive.Config{
				Provider: "google",
				OAuth: drive.OAuthConfig{
					ClientID:     "54321",
					ClientSecret: "edcba",
					Scopes: []string{
						"https://www.googleapis.com/auth/drive",
					},
					TokenPath: "/dev/null",
				},
				FileParentID:  "1",
				ChunkParentID: "2",
				Write:         false,
			},
		},
		{
			name:       "single local provider",
			configPath: "testdata/single-local-provider.config.json",
			want: drive.Config{
				Provider:      "local",
				FileParentID:  "/tmp/shade/files",
				ChunkParentID: "/tmp/shade/chunks",
				Write:         true,
				MaxFiles:      10000,
				MaxChunkBytes: 50000000000,
			},
		},
		{
			name:       "single memory provider",
			configPath: "testdata/single-memory-provider.config.json",
			want: drive.Config{
				Provider:      "memory",
				Write:         true,
				MaxFiles:      10000,
				MaxChunkBytes: 50000000,
			},
		},
		{
			name:       "cache holding one provider",
			configPath: "testdata/cache-holding-one-provider.config.json",
			want: drive.Config{
				Provider: "cache",
				Children: []drive.Config{
					{
						Provider:      "memory",
						Write:         true,
						MaxFiles:      10000,
						MaxChunkBytes: 50000000,
					},
				},
			},
		},
		{
			name:       "encrypted memory client",
			configPath: "testdata/encrypted-memory-client.config.json",
			want: drive.Config{
				Provider: "encrypt",
				// amusingly short 56bit RSA key, to make the tests easier to read
				RsaPrivateKey: string("-----BEGIN RSA PRIVATE KEY-----\nMDkCAQACCADc5XG/z8hNAgMBAAECB0hGXla5p8ECBA+wdzECBA4UU90CBA4/9MEC\nBALlFRUCBAKTsts=\n-----END RSA PRIVATE KEY-----"),
				Children: []drive.Config{
					{
						Provider:      "memory",
						Write:         true,
						MaxFiles:      10000,
						MaxChunkBytes: 50000000,
					},
				},
			},
		},
	} {
		if err := tc.initialize(); err != nil {
			t.Errorf("couldn't initialize test case %q: %v", tc.name, err)
		} else {
			config, err := parseConfig(tc.config)
			if err != nil && tc.err == "" {
				t.Errorf("test %q wanted error %q but got %q", tc.name, tc.err, err)
			} else if err == nil && tc.err != "" {
				t.Errorf("test %q wanted err: %q, got err: %q", tc.name, tc.err, err)
			} else if err != nil && tc.err != "" && !strings.Contains(err.Error(), tc.err) {
				t.Errorf("test %q wanted err: %q, got err: %q", tc.name, tc.err, err)
			} else if !reflect.DeepEqual(tc.want, config) {
				t.Errorf("test %q\nwanted: %+v\ngot: %+v", tc.name, tc.want, config)
			}
		}
	}
}
