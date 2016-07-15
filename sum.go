package shade

import (
	"crypto/sha256"
	"encoding/hex"
)

// Sum is the uniform hash calculation used for all operations on Shade data.
func Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// SumString returns a string representation of a Shade Sum.
func SumString(data []byte) string {
	return hex.EncodeToString(Sum(data))
}
