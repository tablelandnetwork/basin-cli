package main

import (
	"os"

	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func main() {
	cliApp := &cli.App{
		Name:  "basin",
		Usage: "Publish data from your database to the Tableland network.",
		Commands: []*cli.Command{
			newPublicationCommand(),
			newWalletCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
