package main

import (
	"fmt"
	"os"
	"path"

	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
)

// DefaultProviderHost is the address of Basin Provider.
const DefaultProviderHost = "basin.tableland.xyz:3000"
const DefaultTlockHost = "https://api.drand.sh/"
const DefaultTlockChain = "dbd506d6ef76e5f386f41c651dcb808c5bcbd75471cc4eafa3f4df7ad4e4c493"

type config struct {
	Publications map[string]publication `yaml:"publications"`
}

type publication struct {
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	Host          string `yaml:"host"`
	Port          int    `yaml:"port"`
	Database      string `yaml:"database"`
	ProviderHost  string `yaml:"provider_host"`
	TlockDuration string `yaml:"tlock_duration"`
	TlockHost     string `yaml:"tlock_host"`
	TlockChain    string `yaml:"tlock_chain"`
}

func newConfig() *config {
	return &config{
		Publications: make(map[string]publication),
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

	// Adding defaults for old configs
	for key, pub := range conf.Publications {
		if pub.TlockHost == "" {
			pub.TlockHost = DefaultTlockHost
		}
		if pub.TlockChain == "" {
			pub.TlockChain = DefaultTlockChain
		}
		conf.Publications[key] = pub
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

		dir = path.Join(dir, ".basin")
	}

	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0o755); err != nil {
			return "", fmt.Errorf("mkdir: %s", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("is not exist: %s", err)
	}

	return dir, nil
}
