using Go = import "/go.capnp";

@0x9cf9878fd3dd8473;

$Go.package("basinprovider");
$Go.import("github.com/tablelandnetwork/basin-cli/pkg/basinprovider");

interface Publications {
	create @0 (ns :Text, rel :Text, schema :import "../capnp/definitions.capnp" .Schema, owner :Data) -> (exists :Bool);
	push @1 (ns :Text, rel :Text, tx :import "../capnp/definitions.capnp" .Tx, sig :Data);

	upload @2 (ns :Text, rel :Text, size: UInt64) -> (callback :Callback);
	interface Callback {
		write @0 (chunk :Data);
		done @1 (sig :Data);
	}

    list @3 (owner :Data) -> (publications :List(Text));
}
