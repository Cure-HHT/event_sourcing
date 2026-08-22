// Implements: EVS-PRD-provenance/B
// (pure-functional append producing
//   a new immutable chain without mutating the input)

import 'package:provenance/src/provenance_entry.dart';

/// Append a single [ProvenanceEntry] to the tail of a chain-of-custody.
///
/// Returns a NEW unmodifiable list. The input [chain] is never mutated, and
/// the returned list itself rejects modification so callers cannot break the
/// no-mutation invariant downstream.
///
/// Each hop that receives an event calls `appendHop` exactly once to record
/// its receipt.
List<ProvenanceEntry> appendHop(
  List<ProvenanceEntry> chain,
  ProvenanceEntry entry,
) => List<ProvenanceEntry>.unmodifiable(<ProvenanceEntry>[...chain, entry]);
