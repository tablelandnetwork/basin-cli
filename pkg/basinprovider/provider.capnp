using Go = import "/go.capnp";

@0x9cf9878fd3dd8473;

$Go.package("basinprovider");
$Go.import("github.com/tablelandnetwork/basin-cli/pkg/basinprovider");

interface BasinProviderClient {
	push @0 (tx :import "../capnp/tx.capnp" .Tx, signature :Data) -> (response :UInt64);
}