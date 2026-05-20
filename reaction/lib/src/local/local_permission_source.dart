// Implements: EVS-PRD-permission-snapshot-source/B/E —
// LocalPermissionSource derives the snapshot from the substrate's
// RolePermissionGrants projection (B), and re-fetches + re-emits when
// the active Principal changes via setActivePrincipal (E).
// Subscribes to the role_permission_grants view; builds PermissionSnapshot
// from the active Principal's activeRole by reading the projection rows
// directly via EventStore.backend.findViewRows. Stream honors
// snapshot-on-listen via per-listener forwarding.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import 'package:reaction/src/interfaces/permission_source.dart';

/// In-process [PermissionSource] impl. Recomputes a [PermissionSnapshot]
/// for the active [Principal]'s `activeRole` whenever:
///
/// 1. [setActivePrincipal] changes the active principal, OR
/// 2. The substrate's `role_permission_grants` view emits a [Delta] or
///    [Tombstone] update (permission grant/revoke for any role).
///
/// Grants are read directly from the `role_permission_grants` projection
/// via `eventStore.backend.findViewRows` — the same projection the
/// substrate's [TableBackedAuthorizationPolicy] reads at authorize time.
///
/// `current` is null until [setActivePrincipal] is called (or after
/// clearing with `setActivePrincipal(null)`).
///
/// If the active Principal is [AnonymousPrincipal] (no `activeRole`),
/// `current` is null — the substrate has no notion of anonymous permissions.
///
/// Disposal: call [dispose] to cancel the substrate subscription and close
/// the internal stream controller. After [dispose], the source must not be
/// used.
class LocalPermissionSource implements PermissionSource {
  LocalPermissionSource({required this.eventStore}) {
    _grantsSub = eventStore
        .subscribe<Map<String, Object?>>(
          const SubscriptionFilter(),
          AggregateMode<Map<String, Object?>>(
            viewName: 'role_permission_grants',
            mapper: (m) => m,
          ),
        )
        .listen((update) {
          if (update is Delta<Map<String, Object?>> ||
              update is Tombstone<Map<String, Object?>>) {
            unawaited(_recompute());
          }
        });
  }

  final EventStore eventStore;

  Principal? _principal;
  PermissionSnapshot? _current;
  final StreamController<PermissionSnapshot?> _controller =
      StreamController<PermissionSnapshot?>.broadcast();
  StreamSubscription<Update<Map<String, Object?>>>? _grantsSub;

  @override
  PermissionSnapshot? get current => _current;

  @override
  Stream<PermissionSnapshot?> get stream {
    // Per-listener wrapper: emits _current synchronously on subscribe
    // (snapshot-on-listen contract), then forwards all subsequent updates
    // from the broadcast controller. A dedicated per-listener controller is
    // used so that the forward subscription is held open while the listener
    // is active and no events are lost to broadcast-stream buffering gaps.
    late StreamController<PermissionSnapshot?> per;
    StreamSubscription<PermissionSnapshot?>? forwardSub;
    per = StreamController<PermissionSnapshot?>(
      onListen: () {
        per.add(_current);
        forwardSub = _controller.stream.listen(per.add, onDone: per.close);
      },
      onCancel: () => forwardSub?.cancel(),
    );
    return per.stream;
  }

  /// Set the active principal. Triggers an async snapshot recompute.
  /// Pass null to clear immediately (synchronous).
  void setActivePrincipal(Principal? principal) {
    _principal = principal;
    if (principal == null) {
      _current = null;
      if (!_controller.isClosed) _controller.add(null);
      return;
    }
    unawaited(_recompute());
  }

  Future<void> _recompute() async {
    final p = _principal;
    if (p == null) {
      _setNull();
      return;
    }
    // Extract the active role: UserPrincipal has a non-nullable activeRole;
    // AnonymousPrincipal has no active role concept — treat as null.
    final activeRole = switch (p) {
      UserPrincipal(:final activeRole) => activeRole,
      AnonymousPrincipal() => null,
    };
    if (activeRole == null) {
      _setNull();
      return;
    }
    // Read the role_permission_grants projection directly, filtered to
    // the active role inside a single backend transaction. Each row is
    // {'role': <role>, 'permissionName': <name>} (see
    // rolePermissionGrantsSpec). Scope-class metadata is attached at
    // Action.permissions declaration time, not stored in the grants
    // projection.
    final rows = await eventStore.backend
        .transaction<List<Map<String, dynamic>>>(
          (txn) => eventStore.backend.findViewRowsInTxn(
            txn,
            'role_permission_grants',
            where: <String, Object?>{'role': activeRole},
          ),
        );
    final grants = <Permission>{
      for (final r in rows) Permission(r['permissionName']! as String),
    };
    // Guard against a race: verify the principal is still the same after
    // the async findViewRows call returns.
    if (!identical(_principal, p)) return;
    final snapshot = PermissionSnapshot(
      role: activeRole,
      grants: grants,
      issuedAt: DateTime.now(),
    );
    _current = snapshot;
    if (!_controller.isClosed) _controller.add(snapshot);
  }

  void _setNull() {
    _current = null;
    if (!_controller.isClosed) _controller.add(null);
  }

  @override
  Future<void> dispose() async {
    await _grantsSub?.cancel();
    _grantsSub = null;
    if (!_controller.isClosed) await _controller.close();
  }
}
