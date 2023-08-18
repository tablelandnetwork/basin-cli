using Go = import "/go.capnp";

@0x9cf9878fd3dd8473;

$Go.package("basinprovider");
$Go.import("pkg/basinprovider");

interface BasinProviderClient {
	push @0 (txData :Data, signature :Data) -> (response :UInt64);
}