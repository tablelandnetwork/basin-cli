package app

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

func TestBasinUploader(t *testing.T) {
	// used for testing
	pk := "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	mock := &basinProviderUploderMock{}

	uploader := NewBasinUploader("test", "test", mock, privateKey)

	buf := bytes.NewBuffer(make([]byte, 0, 10))
	err = uploader.Upload(context.Background(), "testdata/test.parquet", buf)
	require.NoError(t, err)
	// there's no much logic to test in this component apart from the fact that the file was read
	// the test file has 113629 bytes
	require.Equal(t, 113629, mock.bytesRead)
}

type basinProviderUploderMock struct {
	bytesRead int
}

func (bp *basinProviderUploderMock) Upload(
	_ context.Context, _ string, _ string, r io.Reader, _ *Signer, _ io.Writer,
) error {
	buf := make([]byte, 4*1024)
	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		bp.bytesRead += n
	}

	return nil
}
