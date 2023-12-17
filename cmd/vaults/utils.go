package main

import (
	"encoding/json"
	"os"
)

type version struct {
	Version string `json:"version"`
}

func getVersion() string {
	var v version
	data, err := os.ReadFile("version.json")
	if err != nil {
		return "unknown"
	}
	err = json.Unmarshal(data, &v)
	if err != nil {
		return "unknown"
	}
	return v.Version
}
