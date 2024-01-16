package app

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestRetrieverFileOutput(t *testing.T) {
	retriever := NewRetriever(&vaultsProviderMock{})
	output := t.TempDir()
	err := retriever.Retrieve(context.Background(), cid.Cid{}, output, "test.txt")
	require.NoError(t, err)

	f, err := os.Open(path.Join(output, "test.txt"))
	require.NoError(t, err)

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	require.Equal(t, []byte("Hello"), data)
}

func TestRetrieverStdoutOutput(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w // overwrite os.Stdout so we can read from it

	retriever := NewRetriever(&vaultsProviderMock{})

	err := retriever.Retrieve(context.Background(), cid.Cid{}, "-", "test.txt")
	require.NoError(t, err)

	_ = w.Close()
	data, _ := io.ReadAll(r)
	os.Stdout = old

	require.Equal(t, []byte("Hello"), data)
}
