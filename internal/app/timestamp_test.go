package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	ts, err := ParseTimestamp("")
	require.NoError(t, err)
	require.Equal(t, Timestamp{}, ts)

	ts, err = ParseTimestamp("917755885")
	require.NoError(t, err)
	require.Equal(t, Timestamp{t: time.Unix(917755885, 0).UTC()}, ts)

	ts, err = ParseTimestamp("2000-07-13")
	require.NoError(t, err)
	require.Equal(t, Timestamp{t: time.Unix(963446400, 0).UTC()}, ts)

	ts, err = ParseTimestamp("1999-01-31T07:11:25+03:00")
	require.NoError(t, err)
	require.Equal(t, Timestamp{t: time.Unix(917755885, 0).UTC()}, ts)
}
