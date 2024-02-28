package signing

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSigner(t *testing.T) {
	privateKey, _ := HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
	signer := NewSigner(privateKey)

	testCases := []struct {
		name              string
		setup             func() (filename string, cleanup func())
		wantErr           string
		expectedSignature string
	}{
		{
			name: "should sign with content",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				content := []byte("data to be signed")
				tmpFile.Write(content)
				tmpFile.Close()
				return name, func() { os.Remove(name) }
			},
			wantErr:           "",
			expectedSignature: "6ddb61a19b9df71136b48c80b2e86e7e20313d5eec0de9210802335b300ba8df6c332d35a5d753a028d703769fd9b66d7ce5902d80369750cf55118b1679d84900",
		},
		{
			name: "should fail with empty file",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				tmpFile.Close()
				return name, func() { os.Remove(name) }
			},
			wantErr: "error with file: content is empty",
		},
		{
			name: "should fail with non-existent file",
			setup: func() (filename string, cleanup func()) {
				tmpFile, _ := os.CreateTemp("", "test_file")
				name := tmpFile.Name()
				tmpFile.Close()
				os.Remove(name)
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
			signature := SignatureBytesToHex(signatureBytes)
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

func TestPrivateKey(t *testing.T) {
	testCases := []struct {
		name    string
		setup   func() (pk string)
		wantErr string
	}{
		{
			name: "should load a real private key",
			setup: func() (pk string) {
				pk = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
				return pk
			},
			wantErr: "",
		},
		{
			name: "should fail to load 0x prefixed key",
			setup: func() (pk string) {
				pk = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
				return pk
			},
			wantErr: "invalid hex character 'x' in private key",
		},
		{
			name: "should fail to load random string",
			setup: func() (pk string) {
				pk = "1234abcd"
				return pk
			},
			wantErr: "invalid length, need 256 bits",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pk := tc.setup()

			hex, err := HexToECDSA(pk)
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
