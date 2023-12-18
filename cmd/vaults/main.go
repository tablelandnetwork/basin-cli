package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func init() {
	cli.VersionFlag = &cli.BoolFlag{Name: "version", Aliases: []string{"V"}, Usage: "show version"} // Enforce uppercase
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("v%s\n", c.App.Version)
	}
}

func main() {
	// migrate v1 config to v2 config
	migrateConfigV1ToV2()
	var version = getVersion()

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
			newRetrieveCommand(),
			newWalletCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
