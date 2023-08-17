package basinprovider

import (
	"context"
	"fmt"
)

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct {
	client BasinProviderClient
}

func NewBasinProvider(c BasinProviderClient) *BasinProvider {
	return &BasinProvider{
		client: c,
	}
}

// Push pushes Postgres tx to the server.
func (bp *BasinProvider) Push(ctx context.Context, payload []byte) error {
	if _, err := bp.client.Push(ctx, &Data{Payload: payload}); err != nil {
		return fmt.Errorf("push: %s", err)
	}
	return nil
}
