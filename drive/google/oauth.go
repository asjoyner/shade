package google

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"

	gdrive "google.golang.org/api/drive/v3"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

const (
	clientID     = "1060319012426-gb2f7picjh3dfsoni203rkmsjliqok1f.apps.googleusercontent.com"
	clientSecret = "jfcCV2384sXBlqdaes26bAA5"
	authURL      = "https://accounts.google.com/o/oauth2/auth"
	tokenURL     = "https://accounts.google.com/o/oauth2/token"
	redirectURI  = "https://localhost"
)

var (
	// scope defaults to requesting r/w access to all Files, Folders, and AppData
	scope = []string{gdrive.DriveAppdataScope, gdrive.DriveFileScope}
	// tokenPath defaults to "google.token" in the config dir, but can be
	// overridden by configuring OAuth.TokenPath
	tokenPath = filepath.Join(shade.ConfigDir(), "google.token")
)

func GetOAuthClient(c drive.Config) *http.Client {
	conf := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       scope,
		Endpoint: oauth2.Endpoint{
			AuthURL:  authURL,
			TokenURL: tokenURL,
		},
		RedirectURL: redirectURI,
	}
	if c.OAuth.ClientID != "" {
		conf.ClientID = c.OAuth.ClientID
	}
	if c.OAuth.ClientSecret != "" {
		conf.ClientSecret = c.OAuth.ClientSecret
	}
	if len(c.OAuth.Scopes) != 0 {
		conf.Scopes = c.OAuth.Scopes
	}
	if c.OAuth.TokenPath != "" {
		tokenPath = c.OAuth.TokenPath
	}
	return getClient(context.TODO(), conf)
}

func getClient(ctx context.Context, config *oauth2.Config) *http.Client {
	tok, err := tokenFromFile(tokenPath)
	if err != nil {
		tok = fetchToken(config)
		saveToken(tokenPath, tok)
	}
	return config.Client(ctx, tok)
}

// fetchToken uses Config to request a Token.
func fetchToken(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Visit this URL in your browser: \n%v\n", authURL)

	var code string
	fmt.Print("Enter your authorization code: ")
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}
	log.Printf("\nRead code: %q\n", code)

	// TODO(cfunkhouser): Get a meaningful context here.
	tok, err := config.Exchange(context.TODO(), code)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(t)
	defer f.Close()
	return t, err
}

func saveToken(file string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", file)
	f, err := os.Create(file)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}
