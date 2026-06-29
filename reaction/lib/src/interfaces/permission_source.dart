// Implements: EVS-PRD-permission-source/A/D — defines the
// PermissionSource interface (A: current synchronous getter +
// Stream<EffectiveAuthorization?> + dispose) and the rule that the
// active Principal is sourced from a co-mounted AuthSession, with no
// principal mutator on this interface (D).
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Per-Principal view of the substrate's permissions surface — i.e.,
/// "what is this user allowed to do, and over which scopes?" The active
/// Principal is sourced from an [AuthSession] (set externally; not on
/// this interface) and used to scope the snapshot.
///
/// The snapshot is the substrate's [EffectiveAuthorization]: active role
/// name, the role's permission set, and the user's scope assignments
/// under that role. UI gating reads `rolePermissions` for coarse
/// "can the user do X at all?" checks and walks `scopeAssignments` for
/// fine "which X-scoped items can this user act on?" pre-filtering.
/// Per-decision authorization still flows through the substrate at
/// dispatch time.
///
/// Two impls ship with `reaction`:
///
/// - [LocalPermissionSource] (in-process): subscribes to the
///   `role_permission_grants` and `user_role_scopes` views and routes
///   through `AuthorizationPolicy.effectivePermissionsFor` to derive
///   the [EffectiveAuthorization] for the active principal.
/// - [RemotePermissionSource] (cross-process): initial
///   HTTP GET `/permissions/snapshot?principalId=...`; subsequent
///   updates via the multiplexed WS subscription on the same
///   projection.
abstract interface class PermissionSource {
  /// Current effective authorization for the active Principal, or
  /// `null` if no Principal is set or the snapshot hasn't loaded yet.
  EffectiveAuthorization? get current;

  /// Emits whenever [current] changes. The first event delivered to a
  /// listener is the current value (snapshot-on-listen), then deltas
  /// as the snapshot updates.
  Stream<EffectiveAuthorization?> get stream;

  /// Force a re-read of the snapshot for the active Principal and emit it on
  /// [stream]. Updates [current] IN PLACE — it does NOT first clear to `null` —
  /// so consumers (e.g. permission gates) never flash a "not-loaded" state
  /// mid-refresh.
  ///
  /// Use when the effective authorization context changes in a way the source
  /// can't observe on its own — e.g. an active-role switch carried as a
  /// per-request credential claim, where the active role changes but the
  /// co-mounted [AuthSession] never leaves Authenticated (so neither the
  /// Authenticated-transition refetch nor a server `stale_data` envelope
  /// fires). No-op when no Principal is active.
  Future<void> refresh();

  /// Release any underlying resources (the stream controller, the
  /// substrate-subscription this source uses internally, etc.).
  /// After [dispose], the source is no longer usable.
  Future<void> dispose();
}
