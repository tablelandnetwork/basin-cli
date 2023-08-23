package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type config struct {
	DBS struct {
		Postgres struct {
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			Database string `yaml:"database"`
		} `yaml:"postgres"`
	} `yaml:"dbs"`
	Address string `yaml:"address"`
}

func loadConfig(path string) (*config, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return &config{}, err
	}

	conf := &config{}
	if err := yaml.Unmarshal(buf, conf); err != nil {
		return &config{}, err
	}

	return conf, nil
}
