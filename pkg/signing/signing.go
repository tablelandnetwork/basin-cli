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

// LoadPrivateKey creates an ecdsa.PrivateKey from a hex-encoded string.
func LoadPrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	return crypto.HexToECDSA(hexKey)
}

// NewSigner creates a new signer with a private key and internal state.
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

// Sign returns the signature of the hash state.
func (s *Signer) signState() ([]byte, error) {
	var h common.Hash
	_, _ = s.state.Read(h[:])
	signature, err := crypto.Sign(h.Bytes(), s.privateKey)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to sign state: %s", err)
	}

	return signature, nil
}

// SignFile returns the signature of a signed file.
func (s *Signer) SignFile(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", err.Error())
	}
	defer file.Close()

	// Check if the file is empty and return an error if it is
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %s", err.Error())
	}
	if info.Size() == 0 {
		return "", fmt.Errorf("failed to create signature: %s", "file is empty")
	}

	nBytes, nChunks := int64(0), int64(0)
	r := bufio.NewReader(file)
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
			return "", fmt.Errorf("read error: %s", err)
		}
		nChunks++
		nBytes += int64(len(buf))

		s.Sum(buf)
	}

	signature, err := s.signState()
	if err != nil {
		return "", fmt.Errorf("failed to sign: %s", err)
	}

	return hex.EncodeToString(signature), nil
}
