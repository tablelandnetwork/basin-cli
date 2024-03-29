package signing

import (
	"crypto/ecdsa"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignFile(t *testing.T) {
	privateKey, _ := HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	signer := NewSigner(privateKey)

	testCases := []struct {
		name              string
		setup             func() (filename string, cleanup func())
		wantErr           string
		expectedSignature string
	}{
		{
			name: "should sign file with content",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				content := []byte("data to be signed")
				_, err := tmpFile.Write(content)
				require.NoError(t, err, "Error writing to file")
				err = tmpFile.Close()
				require.NoError(t, err, "Error closing file")
				return name, func() {
					err = os.Remove(name)
					require.NoError(t, err, "Error removing file")
				}
			},
			wantErr: "",
			expectedSignature: "6ddb61a19b9df71136b48c80b2e86e7e20313d5eec0de9210802335b3" +
				"00ba8df6c332d35a5d753a028d703769fd9b66d7ce5902d80369750cf55118b1679d84900",
		},
		{
			name: "should fail with empty file",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				err := tmpFile.Close()
				require.NoError(t, err, "Error closing file")
				return name, func() {
					err = os.Remove(name)
					require.NoError(t, err, "Error removing file")
				}
			},
			wantErr: "error with file: content is empty",
		},
		{
			name: "should fail with non-existent file",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				err := tmpFile.Close()
				require.NoError(t, err, "Error closing file")
				err = os.Remove(name)
				require.NoError(t, err, "Error removing file")
				return name, func() {}
			},
			wantErr: "error reading [file=",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filename, cleanup := tc.setup()
			defer cleanup()

			signatureBytes, err := signer.SignFile(filename)
			signature := hex.EncodeToString(signatureBytes)
			if tc.wantErr != "" {
				require.Error(t, err, "Expected an error for %v", tc.name)
				require.Contains(t, err.Error(), tc.wantErr, "SignFile() error = %v, wantErr %v", err, tc.wantErr)
			} else {
				require.NoError(t, err, "SignFile() unexpected error = %v", err)
				require.Equal(t, tc.expectedSignature, signature, "Signature mismatch")
			}
		})
	}
}

func TestSignBytes(t *testing.T) {
	privateKey, _ := HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	signer := NewSigner(privateKey)

	testCases := []struct {
		name              string
		content           []byte
		wantErr           string
		expectedSignature string
	}{
		{
			name:    "should sign bytes",
			content: []byte("data to be signed"),
			wantErr: "",
			expectedSignature: "6ddb61a19b9df71136b48c80b2e86e7e20313d5eec0de9210802335b3" +
				"00ba8df6c332d35a5d753a028d703769fd9b66d7ce5902d80369750cf55118b1679d84900",
		},
		{
			name:    "should fail with empty bytes",
			content: []byte(""),
			wantErr: "error with data: content is empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			signatureBytes, err := signer.SignBytes(tc.content)
			signature := hex.EncodeToString(signatureBytes)
			if tc.wantErr != "" {
				require.Error(t, err, "Expected an error for %v", tc.name)
				require.Contains(t, err.Error(), tc.wantErr, "SignBytes() error = %v, wantErr %v", err, tc.wantErr)
			} else {
				require.NoError(t, err, "SignBytes() unexpected error = %v", err)
				require.Equal(t, tc.expectedSignature, signature, "Signature mismatch")
			}
		})
	}
}

func TestPrivateKey(t *testing.T) {
	testCases := []struct {
		name    string
		setup   func() (pk string, filename string, cleanup func())
		wantErr string
	}{
		{
			name: "should load a private key string",
			setup: func() (pk string, filename string, cleanup func()) {
				pk = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
				return pk, "", func() {}
			},
			wantErr: "",
		},
		{
			name: "should load a private key file",
			setup: func() (pk string, filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				content := []byte("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
				_, err := tmpFile.Write(content)
				require.NoError(t, err, "Error writing to file")
				err = tmpFile.Close()
				require.NoError(t, err, "Error closing file")
				return pk, name, func() {
					err = os.Remove(name)
					require.NoError(t, err, "Error removing file")
				}
			},
			wantErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pk, filename, cleanup := tc.setup()
			defer cleanup()

			var hex *ecdsa.PrivateKey
			var err error
			if filename == "" {
				hex, err = HexToECDSA(pk)
			} else {
				hex, err = FileToECDSA(filename)
			}
			if tc.wantErr != "" {
				require.Error(t, err, "Expected an error for %v", tc.name)
				require.EqualErrorf(t, err, tc.wantErr, "HexToECDSA() error = %v, wantErr %v", err, tc.wantErr)
			} else {
				require.NoError(t, err, "HexToECDSA() unexpected error = %v", err)
				require.NotNil(t, hex, "HexToECDSA() returned nil")
			}
		})
	}
}
