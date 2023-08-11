package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/mitchellh/go-homedir"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

func main() {
	zlog.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.TraceLevel).
		With().
		Timestamp().
		Caller().
		Int("pid", os.Getpid()).
		Logger()

	cliApp := &cli.App{
		Name:  "basin",
		Usage: "basin replicates your database as logs and store them in Filecoin",
		Commands: []*cli.Command{
			newSetupCommand(terminal.Stdio{In: os.Stdin, Out: os.Stdout, Err: os.Stderr}),
			newReplicatecommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		log.Fatal(err)
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
