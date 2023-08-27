using Go = import "/go.capnp";

@0x9cf9878fd3dd8473;

$Go.package("basinprovider");
$Go.import("github.com/tablelandnetwork/basin-cli/pkg/basinprovider");

interface Publications {
	create @0 (ns :Text, rel :Text, schema :import "../capnp/definitions.capnp" .Schema, owner :Text);
	push @1 (ns :Text, rel :Text, tx :import "../capnp/definitions.capnp" .Tx, sig :Data);
}
