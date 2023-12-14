package app

import (
	"bufio"
	"context"
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

// BasinUploader contains logic of uploading Parquet files to Basin Provider.
type BasinUploader struct {
	namespace  string
	relation   string
	privateKey *ecdsa.PrivateKey
	provider   BasinProvider
}

// NewBasinUploader creates new uploader.
func NewBasinUploader(
	ns string, rel string, bp BasinProvider, pk *ecdsa.PrivateKey,
) *BasinUploader {
	return &BasinUploader{
		namespace:  ns,
		relation:   rel,
		provider:   bp,
		privateKey: pk,
	}
}

// Upload sends file to provider for upload.
func (bu *BasinUploader) Upload(
	ctx context.Context, filepath string, progress io.Writer, ts Timestamp, sz int64,
) error {
	f, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer func() {
		_ = f.Close()
	}()

	signer := NewSigner(bu.privateKey)
	signature, err := signer.SignFile(filepath)
	if err != nil {
		return fmt.Errorf("signing the file: %s", err)
	}

	params := WriteVaultEventParams{
		Vault:       Vault(fmt.Sprintf("%s.%s", bu.namespace, bu.relation)),
		Timestamp:   ts,
		Content:     f,
		ProgressBar: progress,
		Signature:   hex.EncodeToString(signature),
		Size:        sz,
	}

	if err := bu.provider.WriteVaultEvent(ctx, params); err != nil {
		return fmt.Errorf("write vault event: %s", err)
	}

	return nil
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

// SignFile signs an entire file.
func (s *Signer) SignFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return []byte{}, fmt.Errorf("error to read [file=%v]: %v", filename, err.Error())
	}

	defer func() {
		_ = f.Close()
	}()

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

		s.Sum(buf)

		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
	}

	signature, err := s.Sign()
	if err != nil {
		log.Fatal("failed to sign")
	}

	return signature, nil
}
