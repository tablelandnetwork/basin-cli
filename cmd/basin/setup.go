package main

import (
	"fmt"
	"os"
	"path"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

// TODO: remove later.
func newSetupCommand(s terminal.Stdio) *cli.Command {
	return &cli.Command{
		Name:  "setup",
		Usage: "sets up basin",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dir",
				Usage: "The directory where config will be stored (default: $HOME)",
			},
		},
		Action: func(cCtx *cli.Context) error {
			answers := struct {
				Host       string
				Port       int
				User       string
				Password   string
				Database   string
				PrivateKey string
			}{}

			// the questions to ask
			qs := []*survey.Question{
				{
					Name: "host",
					Prompt: &survey.Input{
						Message: "Enter your Postgres host: ",
					},
					Validate: survey.Required,
				},
				{
					Name: "port",
					Prompt: &survey.Input{
						Message: "Enter your Postgres port: ",
						Default: "5432",
					},
					Validate: survey.Required,
				},
				{
					Name: "user",
					Prompt: &survey.Input{
						Message: "Enter your Postgres user:",
					},
					Validate: survey.Required,
				},
				{
					Name: "password",
					Prompt: &survey.Input{
						Message: "Enter your Postgres user's password:",
					},
				},
				{
					Name: "database",
					Prompt: &survey.Input{
						Message: "Enter your Postgres database:",
					},
					Validate: survey.Required,
				},
				{
					Name: "privatekey",
					Prompt: &survey.Input{
						Message: "Enter your Private Key:",
					},
					Validate: survey.Required,
				},
			}

			if err := survey.Ask(qs, &answers, survey.WithStdio(s.In, s.Out, s.Err)); err != nil {
				return fmt.Errorf("survey ask: %s", err)
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg := config{}
			f, err := os.Create(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("os create: %s", err)
			}

			cfg.DBS.Postgres.Host = answers.Host
			cfg.DBS.Postgres.Port = answers.Port
			cfg.DBS.Postgres.User = answers.User
			cfg.DBS.Postgres.Password = answers.Password
			cfg.DBS.Postgres.Database = answers.Database

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			bytes, err := yaml.Marshal(cfg)
			if err != nil {
				return fmt.Errorf("marshal: %s", err)
			}

			fmt.Fprintln(s.Out)
			fmt.Fprintln(s.Out)
			fmt.Fprintf(s.Out, "\033[32mSuccess!\033[0m")
			fmt.Fprintln(s.Out)
			fmt.Fprintln(s.Out)

			fmt.Fprint(s.Out, string(bytes))
			fmt.Fprintln(s.Out)
			fmt.Fprintf(s.Out, "\033[32mwritten to %s\033[0m", path.Join(dir, "config.yaml"))
			fmt.Fprintln(s.Out)
			fmt.Fprintln(s.Out)

			return nil
		},
	}
}
