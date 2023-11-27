package app

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Timestamp represents a time a file was uploaded.
type Timestamp struct {
	t time.Time
}

// Seconds returns the timestamp in seconds.
func (t *Timestamp) Seconds() int64 {
	return t.t.Unix()
}

// ParseTimestamp parses a string and returns a time.Time object as UTC.
// It accepts 3 kinds of formats:
// - Integers: that will be parsed as seconds
// - Date Only format (e.g. 2006-01-02)
// - RFC3339 (e.g. 2006-01-02T15:04:05Z07:00).
func ParseTimestamp(ts string) (Timestamp, error) {
	if strings.EqualFold(ts, "") {
		return Timestamp{}, nil
	}

	if n, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return Timestamp{t: time.Unix(n, 0).UTC()}, nil
	}

	if t, err := time.Parse(time.DateOnly, ts); err == nil {
		return Timestamp{t.UTC()}, nil
	}

	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return Timestamp{t.UTC()}, nil
	}
	fmt.Println(time.Parse(time.RFC3339, ts))

	return Timestamp{}, fmt.Errorf("could not parse %s", ts)
}
