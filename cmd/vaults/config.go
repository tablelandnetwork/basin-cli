package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
)

// DefaultProviderHost is the address of Vaults Provider.
const DefaultProviderHost = "https://basin.tableland.xyz"

// DefaultWindowSize is the number of seconds for which WAL updates
// are buffered before being sent to the provider.
const DefaultWindowSize = 3600

type config struct {
	Vaults map[string]vault `yaml:"vaults"`
}

type vault struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Database     string `yaml:"database"`
	ProviderHost string `yaml:"provider_host"`
	WindowSize   int64  `yaml:"window_size"`
}

func newConfig() *config {
	return &config{
		Vaults: make(map[string]vault),
	}
}

func loadConfig(path string) (*config, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return &config{}, err
	}

	conf := newConfig()
	if err := yaml.Unmarshal(buf, conf); err != nil {
		return &config{}, err
	}

	return conf, nil
}

func defaultConfigLocation(dir string) (string, error) {
	if dir == "" {
		// the default directory is home
		var err error
		dir, err = homedir.Dir()
		if err != nil {
			return "", fmt.Errorf("home dir: %s", err)
		}

		dir = path.Join(dir, ".vaults")
	}

	// ignore err if dir already exists
	if err := os.Mkdir(dir, 0o755); err != nil {
		if !strings.Contains(err.Error(), "file exists") {
			return "", fmt.Errorf("mkdir: %s", err)
		}
	}

	return dir, nil
}
