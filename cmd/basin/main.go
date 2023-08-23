package main

import (
	"fmt"
	"os"
	"path"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func main() {
	cliApp := &cli.App{
		Name:  "basin",
		Usage: "basin replicates your database as logs and store them in Filecoin",
		Commands: []*cli.Command{
			newPublication(),
			newWalletCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
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
