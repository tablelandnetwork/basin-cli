using Go = import "/go.capnp";

@0x9cf9878fd3dd8473;

$Go.package("basinprovider");
$Go.import("github.com/tablelandnetwork/basin-cli/pkg/basinprovider");

interface BasinProviderClient {
	create @0 (name :Text, owner :Text, schema :import "../capnp/definitions.capnp" .Schema);
	push @1 (pubName :Text, tx :import "../capnp/definitions.capnp" .Tx, signature :Data) -> (response :UInt64);
}
