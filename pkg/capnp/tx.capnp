using Go = import "/go.capnp";
@0x8c49da2775b6e7db;
$Go.package("capnp");
$Go.import("pkg/capnp");

struct Tx {
    commitLSN @0 :UInt64;

    # TODO: add the rest of the fields
}
