pub(crate) mod scheduler {
    // Generated code. prost expands a `oneof` into an enum whose variants are
    // whatever the messages behind them weigh, so their size spread is decided
    // by the .proto and not by anything reachable from here: `AcquireSandbox`'s
    // outcome carries a whole registry row in one arm and a node id in the
    // others, and adding a field to the row widens the gap. Boxing it is not
    // ours to do — the file is regenerated on every build.
    #![allow(clippy::large_enum_variant)]

    tonic::include_proto!("scheduler.v1");
}
