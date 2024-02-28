package signing

import (
	"bufio"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

// Signer allows you to sign a big stream of bytes by calling Sum multiple times, then Sign.
type Signer struct {
	state      crypto.KeccakState
	privateKey *ecdsa.PrivateKey
}

// HexToECDSA parses a hex encoded private key to an ECDSA private key.
func HexToECDSA(hexKey string) (*ecdsa.PrivateKey, error) {
	return crypto.HexToECDSA(hexKey)
}

// NewSigner creates a new signer.
func NewSigner(pk *ecdsa.PrivateKey) *Signer {
	return &Signer{
		state:      sha3.NewLegacyKeccak256().(crypto.KeccakState),
		privateKey: pk,
	}
}

// Sum updates the hash state with a new chunk.
func (s *Signer) Sum(chunk []byte) {
	s.state.Write(chunk)
}

// Sign signs the internal state.
func (s *Signer) Sign() ([]byte, error) {
	var h common.Hash
	_, _ = s.state.Read(h[:])
	signature, err := crypto.Sign(h.Bytes(), s.privateKey)
	if err != nil {
		return []byte{}, fmt.Errorf("sign: %s", err)
	}

	return signature, nil
}

// SignFile signs an entire file, returning the signature as a byte slice.
func (s *Signer) SignFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return []byte{}, fmt.Errorf("error reading [file=%v]: %v", filename, err.Error())
	}
	defer func() {
		_ = f.Close()
	}()

	// Check if the file is empty and return an error if it is
	info, err := f.Stat()
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get file info: %s", err.Error())
	}
	if info.Size() == 0 {
		return []byte{}, fmt.Errorf("error with file: %s", "content is empty")
	}

	nBytes, nChunks := int64(0), int64(0)
	r := bufio.NewReader(f)
	buf := make([]byte, 0, 4*1024) // 4KB buffer
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return []byte{}, fmt.Errorf("unexpected error reading file: %s", err.Error())
		}
		nChunks++
		nBytes += int64(len(buf))

		s.Sum(buf)

		if err != nil && err != io.EOF {
			return []byte{}, fmt.Errorf("error in buffer: %s", err.Error())
		}
	}

	signature, err := s.Sign()
	if err != nil {
		return []byte{}, fmt.Errorf("failed to sign [file=%v]: %s", filename, err.Error())
	}

	return signature, nil
}

// signatureBytesToHex converts a byte slice to a hex-encoded string.
func SignatureBytesToHex(b []byte) string {
	return hex.EncodeToString(b)
}
