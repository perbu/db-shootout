package keyset

import (
	"fmt"
	"math/rand"
	"time"
	"unsafe"
)

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// RandBytes generates a random string-like byte slice of the given size.
// stupidly fast.
func RandString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}

// GenerateKey generates a deterministic key for the given index.
func GenerateKey(index int) string {
	return fmt.Sprintf("file_%04d", index)
}

// GenerateInvalidKey generates a deterministic key for the given index, but invalid.
func GenerateInvalidKey(index int) string {
	return fmt.Sprintf("file_%04d.invalid", index)
}

// GenerateRandomContent generates a random string of the given size in bytes.
func GenerateRandomContent(size int) string {
	return RandString(size)
}
