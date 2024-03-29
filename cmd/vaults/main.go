package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func init() {
	// Enforce uppercase version shorthand flag
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "show version",
	}
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("%s\n", c.App.Version)
	}
}

var version = "dev"

func main() {
	cliApp := &cli.App{
		Name:    "vaults",
		Usage:   "Continuously publish data from your database or file uploads to the Textile Vaults network.",
		Version: version,
		Commands: []*cli.Command{
			newVaultCreateCommand(),
			newStreamCommand(),
			newWriteCommand(),
			newListCommand(),
			newListEventsCommand(),
			newSignCommand(),
			newRetrieveCommand(),
			newWalletCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
