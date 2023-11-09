package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

// BasinProviderUploader ...
type BasinProviderUploader interface {
	Upload(context.Context, string, string, uint64, io.Reader, *Signer, io.Writer, int64) error
}

// BasinUploader contains logic of uploading Parquet files to Basin Provider.
type BasinUploader struct {
	namespace  string
	relation   string
	privateKey *ecdsa.PrivateKey
	provider   BasinProviderUploader
	timestamp  int64
}

// NewBasinUploader creates new uploader.
func NewBasinUploader(
	ns string, rel string, bp BasinProviderUploader, pk *ecdsa.PrivateKey, timestamp int64,
) *BasinUploader {
	return &BasinUploader{
		namespace:  ns,
		relation:   rel,
		provider:   bp,
		privateKey: pk,
		timestamp:  timestamp,
	}
}

// Upload sends file to provider for upload.
func (bu *BasinUploader) Upload(ctx context.Context, filepath string, progress io.Writer) error {
	f, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer func() {
		_ = f.Close()
	}()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat: %s", err)
	}

	if err := bu.provider.Upload(
		ctx, bu.namespace, bu.relation, uint64(fi.Size()), f, NewSigner(bu.privateKey), progress, bu.timestamp,
	); err != nil {
		return fmt.Errorf("upload: %s", err)
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
