package basinprovider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPush(t *testing.T) {
	bp := &BasinProvider{}
	err := bp.Push([]byte{})
	require.NoError(t, err)
}
