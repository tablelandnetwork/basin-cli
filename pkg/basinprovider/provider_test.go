package basinprovider

import (
	"context"
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestBasinProvider_Push(t *testing.T) {
	bp, closer := server(context.Background())
	defer closer()

	err := bp.Push(context.Background(), []byte{})
	require.NoError(t, err)
}

func server(ctx context.Context) (*BasinProvider, func()) {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	baseServer := grpc.NewServer()
	RegisterBasinProviderServer(baseServer, &basinServer{})

	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	closer := func() {
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	client := NewBasinProviderClient(conn)
	return NewBasinProvider(client), closer
}

type basinServer struct{}

func (s *basinServer) Push(context.Context, *Data) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *basinServer) mustEmbedUnimplementedBasinProviderServer() {}
