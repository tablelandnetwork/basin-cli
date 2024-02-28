package app

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestRetrieverFileOutput(t *testing.T) {
	retriever := NewRetriever(&vaultsProviderMock{}, 0)
	output, err := os.CreateTemp("", "")
	require.NoError(t, err)
	cid := cid.Cid{}
	err = retriever.Retrieve(context.Background(), cid, output.Name())
	require.NoError(t, err)

	_, _ = output.Seek(0, 0)
	data, err := io.ReadAll(output)
	require.NoError(t, err)

	require.Equal(t, []byte("Hello"), data)
}

func TestRetrieverStdoutOutput(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w // overwrite os.Stdout so we can read from it

	retriever := NewRetriever(&vaultsProviderMock{}, 0)

	err := retriever.Retrieve(context.Background(), cid.Cid{}, "-")
	require.NoError(t, err)

	_ = w.Close()
	data, _ := io.ReadAll(r)
	os.Stdout = old

	require.Equal(t, []byte("Hello"), data)
}
