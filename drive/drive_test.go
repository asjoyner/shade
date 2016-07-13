package drive

import (
	"strings"
	"testing"
)

func TestProviderRegistration(t *testing.T) {
	var called bool
	nc := func(c Config) (client Client, err error) {
		called = true
		return
	}
	RegisterProvider("foodrive", nc)
	if !ValidProvider("foodrive") {
		t.Fatalf(`expected "foodrive" to be a valid provider, but wasn't`)
	}

	_, err := NewClient(Config{Provider: "foodrive"})
	if err != nil {
		t.Errorf(`expected NewClient to succeed for "foodrive" provider, but got error: %q`, err)
	}
	if !called {
		t.Errorf(`new client constructor was not called as expected by NewClient("foodrive")`)
	}

	wantErr := `unknown provider: "bardrive"`
	_, err = NewClient(Config{Provider: "bardrive"})
	if err == nil {
		t.Errorf(`expected NewClient("bardrive") to fail, but it did not.`)
	} else if !strings.Contains(err.Error(), wantErr) {
		t.Errorf(`expected NewClient("bardrive") to fail with error %q, but got: %q`, wantErr, err)
	}
}
