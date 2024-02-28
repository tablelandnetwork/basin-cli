package app

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tablelandnetwork/basin-cli/pkg/signing"
)

// VaultsUploader contains logic of uploading Parquet files to Vaults Provider.
type VaultsUploader struct {
	namespace  string
	relation   string
	privateKey *ecdsa.PrivateKey
	provider   VaultsProvider
}

// NewVaultsUploader creates new uploader.
func NewVaultsUploader(
	ns string, rel string, bp VaultsProvider, pk *ecdsa.PrivateKey,
) *VaultsUploader {
	return &VaultsUploader{
		namespace:  ns,
		relation:   rel,
		provider:   bp,
		privateKey: pk,
	}
}

// Upload sends file to provider for upload.
func (bu *VaultsUploader) Upload(
	ctx context.Context, filepath string, progress io.Writer, ts Timestamp, sz int64,
) error {
	f, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer func() {
		_ = f.Close()
	}()

	signer := signing.NewSigner(bu.privateKey)
	signatureBytes, err := signer.SignFile(filepath)
	if err != nil {
		return fmt.Errorf("signing the file: %s", err)
	}
	signature := hex.EncodeToString(signatureBytes)

	filename := filepath
	if strings.Contains(filepath, "/") {
		parts := strings.Split(filepath, "/")
		filename = parts[len(parts)-1]
	}

	params := WriteVaultEventParams{
		Vault:       Vault(fmt.Sprintf("%s.%s", bu.namespace, bu.relation)),
		Timestamp:   ts,
		Content:     f,
		Filename:    filename,
		ProgressBar: progress,
		Signature:   signature,
		Size:        sz,
	}

	if err := bu.provider.WriteVaultEvent(ctx, params); err != nil {
		return fmt.Errorf("write vault event: %s", err)
	}

	return nil
}
