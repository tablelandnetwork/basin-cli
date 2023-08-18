package basinprovider

import (
	"context"
	"fmt"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct {
	client BasinProviderClient
}

func New(c BasinProviderClient) *BasinProvider {
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

type ClientMock struct{}

func (c *ClientMock) Push(ctx context.Context, in *Data, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
