package main

import (
	"context"
	"log"
	"net"
	"os"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
)

func main() {
	client := basinprovider.BasinProviderClient_ServerToClient(basinprovider.NewBasinServerMock(os.Getenv("ETH_ADDRESS")))

	ctx := context.Background()
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		log.Fatal(err)
	}
	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), &rpc.Options{
		BootstrapClient: capnp.Client(client),
	})
	defer conn.Close()

	// Block until the connection terminates.
	select {
	case <-rpcConn.Done():
		os.Exit(0)
	case <-ctx.Done():
		conn.Close()
	}
}
