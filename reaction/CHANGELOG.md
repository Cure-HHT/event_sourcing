# Changelog

## 0.1.0-dev (unreleased)

- `UpdateCodec` gains wire shapes for the substrate's `Pending<T>` variant
  (`type: "pending"`, `aggregateId`, `sequence` always 0) and for
  `EndOfReplay<T>`'s new `state` field (`type: "end_of_replay"`, `state`:
  `"current"` or `"converging"`), preserving both fields end to end for a
  remote consumer exactly as an in-process subscriber sees them.
  `RemoteViewSource` maps both across the consumer-supplied mapper.
