// Implements: EVS-PRD-permission-snapshot-source — defines the
// PermissionSource transport-agnostic interface, including its
// snapshot-on-listen contract on the stream getter.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Per-Principal view of the substrate's `RolePermissionGrants`
/// projection — i.e., "what is this user allowed to do?" The active
/// Principal is sourced from an [AuthSession] (set externally; not on
/// this interface) and used to scope the snapshot.
///
/// Two impls ship with `reaction`:
///
/// - [LocalPermissionSource] (in-process): wraps existing
///   `RoleMatrixReader` + `PermissionSnapshot` machinery; subscribes
///   to the `RolePermissionGrants` view via local `subscribe<T>`;
///   recomputes the snapshot when relevant rows change.
/// - [RemotePermissionSource] (cross-process; Plan B-remote): initial
///   HTTP GET `/permissions/snapshot?principalId=...`; subsequent
///   updates via the multiplexed WS subscription on the same
///   projection.
abstract interface class PermissionSource {
  /// Current snapshot for the active Principal, or `null` if no
  /// Principal is set or the snapshot hasn't loaded yet.
  PermissionSnapshot? get current;

  /// Emits whenever [current] changes. The first event delivered to a
  /// listener is the current value (snapshot-on-listen), then deltas
  /// as the snapshot updates.
  Stream<PermissionSnapshot?> get stream;

  /// Release any underlying resources (the stream controller, the
  /// substrate-subscription this source uses internally, etc.).
  /// After [dispose], the source is no longer usable.
  Future<void> dispose();
}
