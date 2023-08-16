package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/AlecAivazis/survey/v2/core"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/Netflix/go-expect"
	pseudotty "github.com/creack/pty"
	"github.com/hinshun/vt10x"
	"github.com/urfave/cli/v2"
)

func init() {
	// disable color output for all prompts to simplify testing
	core.DisableColor = true
}

func TestInitCommand(t *testing.T) {
	RunTest(t, func(c expectConsole) {
		c.ExpectString("Enter your Postgres host: ")
		c.SendLine("127.0.0.1")
		c.ExpectString("Enter your Postgres port:")
		c.SendLine("5432")
		c.ExpectString("Enter your Postgres user:")
		c.SendLine("postgres")
		c.ExpectString("Enter your Postgres user's password:")
		c.SendLine("")
		c.ExpectString("Enter your Postgres database:")
		c.SendLine("db")
		c.ExpectString("\033[32mSuccess!\033[0m")
		c.ExpectString("dbs:")
		c.ExpectString("postgres:")
		c.ExpectString("user: postgres")
		c.ExpectString("password:")
		c.ExpectString("host: 127.0.0.1")
		c.ExpectString("port: 5432")
		c.ExpectString("database: db")

		dir, _ := defaultConfigLocation("")
		c.ExpectString(fmt.Sprintf("written to %s/config.yaml", dir))

		c.ExpectEOF()
	}, func(s terminal.Stdio) error {
		cliApp := &cli.App{
			Commands: []*cli.Command{
				newSetupCommand(s),
			},
		}

		args := os.Args[0:1] // Name of the program.

		return cliApp.Run(append(args, "setup"))
	})
}

func RunTest(t *testing.T, procedure func(expectConsole), test func(terminal.Stdio) error) {
	t.Helper()
	t.Parallel()

	pty, tty, err := pseudotty.Open()
	if err != nil {
		t.Fatalf("failed to open pseudotty: %v", err)
	}

	term := vt10x.New(vt10x.WithWriter(tty))
	c, err := expect.NewConsole(expect.WithStdin(pty), expect.WithStdout(term), expect.WithCloser(pty, tty))
	if err != nil {
		t.Fatalf("failed to create console: %v", err)
	}
	defer func() { _ = c.Close() }()

	donec := make(chan struct{})
	go func() {
		defer close(donec)
		procedure(&consoleWithErrorHandling{console: c, t: t})
	}()

	stdio := terminal.Stdio{In: c.Tty(), Out: c.Tty(), Err: c.Tty()}
	if err := test(stdio); err != nil {
		t.Error(err)
	}

	if err := c.Tty().Close(); err != nil {
		t.Errorf("error closing Tty: %v", err)
	}
	<-donec
}

type expectConsole interface {
	ExpectString(string)
	ExpectEOF()
	SendLine(string)
	Send(string)
}

type consoleWithErrorHandling struct {
	console *expect.Console
	t       *testing.T
}

func (c *consoleWithErrorHandling) ExpectString(s string) {
	if _, err := c.console.ExpectString(s); err != nil {
		c.t.Helper()
		c.t.Fatalf("ExpectString(%q) = %v", s, err)
	}
}

func (c *consoleWithErrorHandling) SendLine(s string) {
	if _, err := c.console.SendLine(s); err != nil {
		c.t.Helper()
		c.t.Fatalf("SendLine(%q) = %v", s, err)
	}
}

func (c *consoleWithErrorHandling) Send(s string) {
	if _, err := c.console.Send(s); err != nil {
		c.t.Helper()
		c.t.Fatalf("Send(%q) = %v", s, err)
	}
}

func (c *consoleWithErrorHandling) ExpectEOF() {
	if _, err := c.console.ExpectEOF(); err != nil {
		c.t.Helper()
		c.t.Fatalf("ExpectEOF() = %v", err)
	}
}
