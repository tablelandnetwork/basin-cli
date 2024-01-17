package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestRetrieverFileOutput(t *testing.T) {
	retriever := NewRetriever(&vaultsProviderMock{}, true, 0)
	output := t.TempDir()
	cid := cid.Cid{}
	err := retriever.Retrieve(context.Background(), cid, output)
	require.NoError(t, err)

	f, err := os.Open(path.Join(output, fmt.Sprintf("%s-%s", cid.String(), "sample.txt")))
	require.NoError(t, err)

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	require.Equal(t, []byte("Hello"), data)
}

func TestRetrieverStdoutOutput(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w // overwrite os.Stdout so we can read from it

	retriever := NewRetriever(&vaultsProviderMock{}, true, 0)

	err := retriever.Retrieve(context.Background(), cid.Cid{}, "-")
	require.NoError(t, err)

	_ = w.Close()
	data, _ := io.ReadAll(r)
	os.Stdout = old

	require.Equal(t, []byte("Hello"), data)
}
