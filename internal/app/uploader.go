package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tablelandnetwork/basin-cli/pkg/ecmh"
)

// BasinProviderUploader ...
type BasinProviderUploader interface {
	Upload(context.Context, string, string, uint64, io.Reader, *Signer, io.Writer, Timestamp) error
}

// BasinUploader contains logic of uploading Parquet files to Basin Provider.
type BasinUploader struct {
	namespace  string
	relation   string
	privateKey *ecdsa.PrivateKey
	provider   BasinProviderUploader
}

// NewBasinUploader creates new uploader.
func NewBasinUploader(
	ns string, rel string, bp BasinProviderUploader, pk *ecdsa.PrivateKey,
) *BasinUploader {
	return &BasinUploader{
		namespace:  ns,
		relation:   rel,
		provider:   bp,
		privateKey: pk,
	}
}

// Upload sends file to provider for upload.
func (bu *BasinUploader) Upload(ctx context.Context, filepath string, progress io.Writer, ts Timestamp) error {
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
		ctx, bu.namespace, bu.relation, uint64(fi.Size()), f, NewSigner(bu.privateKey), progress, ts,
	); err != nil {
		return fmt.Errorf("upload: %s", err)
	}

	return nil
}

// Signer allows you to sign a big stream of bytes by calling Sum multiple times, then Sign.
type Signer struct {
	state      *ecmh.MultisetHash
	privateKey *ecdsa.PrivateKey
}

// NewSigner creates a new signer.
func NewSigner(pk *ecdsa.PrivateKey) *Signer {
	return &Signer{
		state:      ecmh.NewMultisetHash(),
		privateKey: pk,
	}
}

// Sum updates the hash state with a new chunk.
func (s *Signer) Sum(chunk []byte) {
	s.state.Insert(chunk)
}

// Sign signs the internal state.
func (s *Signer) Sign() ([]byte, error) {
	signature, err := crypto.Sign(s.state.Bytes(), s.privateKey)
	if err != nil {
		return []byte{}, fmt.Errorf("sign: %s", err)
	}

	return signature, nil
}
