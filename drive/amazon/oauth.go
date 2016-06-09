package amazon

import (
	"bufio"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/asjoyner/oauthutil"
	"github.com/asjoyner/shade"
	"github.com/asjoyner/shade/drive"
	"golang.org/x/oauth2"
)

const (
	clientID     string = "amzn1.application-oa2-client.abb62fdbdc574d3b84cc84b5211ce6af"
	clientSecret string = "56b4f6116d98b0570ac174fe5051b55149d06221aec0a5b5508f298aca927b6c"
	scope        string = "clouddrive:read_other clouddrive:write"
	authURL      string = "https://www.amazon.com/ap/oa"
	tokenURL     string = "https://api.amazon.com/auth/o2/token"
	redirectURI  string = "https://localhost"
)

func getOAuthClient(c drive.Config) (*http.Client, error) {
	// Setup sensible defaults for the OAuth config
	conf := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		// NB: CloudDrive scopes must be space separated in a single parameter, not separate parameters!
		Scopes: []string{scope},
		Endpoint: oauth2.Endpoint{
			AuthURL:  authURL,
			TokenURL: tokenURL,
		},
		RedirectURL: redirectURI,
	}

	// Allow the config to override those defaults
	if c.OAuth.ClientID != "" {
		conf.ClientID = c.OAuth.ClientID
	}
	if c.OAuth.ClientSecret != "" {
		conf.ClientSecret = c.OAuth.ClientSecret
	}
	if c.OAuth.Scopes != nil {
		if len(c.OAuth.Scopes) > 1 {
			return nil, fmt.Errorf("clouddrive requires scopes to be space separated, not multiple parameters, see examples/config.json")
		}
		conf.Scopes = c.OAuth.Scopes
	}

	// Grab a cached token if one exists, fetch a fresh one if not
	tokenPath := c.OAuth.TokenPath
	if tokenPath == "" {
		tokenPath = path.Join(shade.ConfigDir(), "amazon.token")
	}
	token, err := oauthutil.TokenFromFile(tokenPath)
	if err != nil {
		token, err = getFreshToken(conf)
		if err != nil {
			return nil, err
		}
		oauthutil.SaveToken(tokenPath, token)
	}

	return conf.Client(oauth2.NoContext, token), nil
}

func getFreshToken(conf *oauth2.Config) (*oauth2.Token, error) {
	// Build the authorization request parameters
	v := url.Values{}
	v.Set("client_id", conf.ClientID)
	for _, s := range conf.Scopes {
		v.Add("scope", s)
	}
	v.Set("response_type", "code")
	v.Set("redirect_uri", redirectURI)

	// Ask the user to authorize shade, receive authorization code
	fmt.Println("Please visit this URL:")
	fmt.Printf("%s?%s\n", authURL, v.Encode())
	fmt.Println("Authorize shade and paste the 'localhost' URL you are redirected to here:")
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("reading response URL: %s", err)
	}
	resp, err := url.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parsing response URL: %s", err)
	}
	code := resp.Query().Get("code")

	// Pass that authorization code to Amazon for access and refresh tokens
	token, err := conf.Exchange(oauth2.NoContext, code)
	if err != nil {
		return nil, fmt.Errorf("failed auth to amazon: %s", err)
	}

	return token, nil
}
