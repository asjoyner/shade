package genkeys

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"strings"

	"github.com/google/subcommands"
)

func init() {
	subcommands.Register(&genkeysCmd{}, "")
}

var (
	keySize = 2048
)

type genkeysCmd struct {
	long    bool
	keySize int
}

func (*genkeysCmd) Name() string     { return "genkeys" }
func (*genkeysCmd) Synopsis() string { return "Generate an JSON-formatted RSA key pair." }
func (*genkeysCmd) Usage() string {
	return `genkeys:
  Print a public and private RSA key pair to STDOUT.
`
}

func (p *genkeysCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.keySize, "s", keySize, "Bit length of RSA keys")
}

func (p *genkeysCmd) Execute(_ context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	key, _ := rsa.GenerateKey(rand.Reader, p.keySize)

	pkcs := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: pkcs})
	fmt.Printf("\n%s\n", stringify(pemBytes))

	pkix, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	pemBytes = pem.EncodeToMemory(&pem.Block{Type: "BEGIN PUBLIC KEY", Bytes: pkix})
	fmt.Printf("\n%s\n\n", stringify(pemBytes))

	return subcommands.ExitSuccess
}

func stringify(b []byte) string {
	s := strings.Replace(string(b), "\n", "\\n", -1)
	s = strings.TrimRight(s, "\\n")
	return s
}
