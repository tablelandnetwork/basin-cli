package main

import (
	"bufio"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		log.Fatal("should have two arguments")
	}

	sk, filename := args[0], args[1]

	privateKey, err := crypto.HexToECDSA(sk)
	if err != nil {
		log.Fatal("invalid private key")
	}

	signer := NewSigner(privateKey)

	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error to read [file=%v]: %v", filename, err.Error())
	}

	nBytes, nChunks := int64(0), int64(0)
	r := bufio.NewReader(f)
	buf := make([]byte, 0, 4*1024)
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
			log.Fatal(err)
		}
		nChunks++
		nBytes += int64(len(buf))

		signer.Sum(buf)

		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
	}

	signature, err := signer.Sign()
	if err != nil {
		log.Fatal("failed to sign")
	}

	fmt.Println("Signature: ", hex.EncodeToString(signature))
}

// Signer allows you to sign a big stream of bytes by calling Sum multiple times, then Sign.
type Signer struct {
	state      crypto.KeccakState
	privateKey *ecdsa.PrivateKey
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
