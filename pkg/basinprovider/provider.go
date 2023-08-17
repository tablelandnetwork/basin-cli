package basinprovider

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct{}

// Push pushes Postgres tx to the server.
func (bp *BasinProvider) Push([]byte) error {
	return nil
}
