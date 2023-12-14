package main

import (
	"os"

	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func main() {
	cliApp := &cli.App{
		Name:  "vaults",
		Usage: "Continuously publish data from your database to the Textile Vaults network.",
		Commands: []*cli.Command{
			newVaultCreateCommand(),
			newStreamCommand(),
			newWriteCommand(),
			newListCommand(),
			newListEventsCommand(),
			newRetrieveCommand(),
			newWalletCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
