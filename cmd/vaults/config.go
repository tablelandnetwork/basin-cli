package main

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/exp/slog"
	"gopkg.in/yaml.v3"
)

// DefaultProviderHost is the address of Vaults Provider.
const DefaultProviderHost = "https://basin.tableland.xyz"

// DefaultWindowSize is the number of seconds for which WAL updates
// are buffered before being sent to the provider.
const DefaultWindowSize = 3600

type config struct {
	Publications map[string]publication `yaml:"publications"`
}

type configV2 struct {
	Vaults map[string]vault `yaml:"vaults"`
}

type publication struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Database     string `yaml:"database"`
	ProviderHost string `yaml:"provider_host"`
	WindowSize   int64  `yaml:"window_size"`
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
		Publications: make(map[string]publication),
	}
}

func newConfigV2() *configV2 {
	return &configV2{
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

func loadConfigV2(path string) (*configV2, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return &configV2{}, err
	}

	conf := newConfigV2()
	if err := yaml.Unmarshal(buf, conf); err != nil {
		return &configV2{}, err
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

func defaultConfigLocationV2(dir string) (string, bool, error) {
	if dir == "" {
		// the default directory is home
		var err error
		dir, err = homedir.Dir()
		if err != nil {
			return "", false, fmt.Errorf("home dir: %s", err)
		}

		dir = path.Join(dir, ".vaults")
	}

	_, err := os.Stat(dir)
	doesNotExist := os.IsNotExist(err)
	if doesNotExist {
		if err := os.Mkdir(dir, 0o755); err != nil {
			return "", doesNotExist, fmt.Errorf("mkdir: %s", err)
		}
	} else if err != nil {
		return "", doesNotExist, fmt.Errorf("is not exist: %s", err)
	}

	return dir, !doesNotExist, nil
}

func migrateConfigV1ToV2() {
	dirV1, err := defaultConfigLocation("")
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	dirV2, exists, err := defaultConfigLocationV2("")
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	// create v2 config.yaml if necessary
	f, err := os.OpenFile(path.Join(dirV2, "config.yaml"), os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = f.Close()
	}()

	if exists {
		return
	}

	// .basin/config.yaml does not exist, there's nothing to migrate
	if _, err := os.Stat(path.Join(dirV1, "config.yaml")); errors.Is(err, os.ErrNotExist) {
		return
	}

	cfgV1, err := loadConfig(path.Join(dirV1, "config.yaml"))
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	cfgV2, err := loadConfigV2(path.Join(dirV2, "config.yaml"))
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for name, item := range cfgV1.Publications {
		cfgV2.Vaults[name] = vault(item)
	}

	if err := yaml.NewEncoder(f).Encode(cfgV2); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
