package fail

import (
	"testing"

	"github.com/asjoyner/shade/drive"
)

func TestLocalFail(t *testing.T) {
	client, err := NewClient(drive.Config{Provider: "fail"})
	if err != nil {
		t.Fatalf("failed to setup fail client... : %s", err)
	}
	if !client.Local() {
		t.Errorf("Fail client with no OAuth config identifies as remote")
	}
}

func TestRemoteFail(t *testing.T) {
	client, err := NewClient(drive.Config{
		Provider: "fail",
		OAuth:    drive.OAuthConfig{ClientID: "remote"},
	})
	if err != nil {
		t.Fatalf("failed to setup fail client... : %s", err)
	}
	if client.Local() {
		t.Errorf("Fail client with no OAuth config identifies as remote")
	}
}
