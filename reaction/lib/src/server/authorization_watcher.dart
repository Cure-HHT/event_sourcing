// Watches the substrate's permission and role-assignment event types.
// On security-narrowing events (role_unassigned, permission_revoked),
// force-logs-out affected users by closing their WS connections with
// code 4003 ("permissions_changed"). On security-expanding events
// (role_assigned, permission_granted) and on opt-in-watched
// containment changes, sends a stale_data envelope (UX-only).
//
// Package-private; not exported from reaction.dart.
//
// Implements: EVS-DEV-authz-watcher/A — role_unassigned -> close
//   4003 permissions_changed for every connection registered for that
//   userId in WsConnectionRegistry.
// Implements: EVS-DEV-authz-watcher/B — permission_revoked from a role
//   R closes 4003 every connection whose registered Principal's
//   activeRole == R (verified via policy.effectivePermissionsFor).
// Implements: EVS-DEV-authz-watcher/C — role_assigned -> stale_data
//   with reason: role_assigned; permission_granted to a held role ->
//   stale_data with reason: permission_added; neither closes the WS.
// Implements: EVS-DEV-authz-watcher/D — containment-projection changes
//   emit no signal by default; watchContainment(aggregateType) opts a
//   projection in and emits stale_data with reason: containment_changed.
// Implements: EVS-DEV-authz-watcher/E — exactly one substrate
//   subscription for the core (role_permission_grant, user_role_scope)
//   event types, plus one per opted-in containment projection; per-
//   connection state lives in WsConnectionRegistry.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:reaction/src/wire/subscription_messages.dart';

/// Watches the substrate's permission and role-assignment event
/// types. On security-narrowing events (role_unassigned,
/// permission_revoked), force-logs-out affected users by closing
/// their WS connections with 4003 permissions_changed. On
/// security-expanding events (role_assigned, permission_granted) and
/// opt-in-watched containment changes, sends a stale_data envelope.
class AuthorizationWatcher {
  AuthorizationWatcher({
    required this.eventStore,
    required this.connectionRegistry,
    required this.policy,
  });

  final EventStore eventStore;
  final WsConnectionRegistry connectionRegistry;
  final AuthorizationPolicy policy;

  StreamSubscription<Update<StoredEvent>>? _coreSub;
  final List<StreamSubscription<Update<StoredEvent>>> _containmentSubs = [];

  /// Start the server-wide watcher subscription.
  Future<void> start() async {
    _coreSub = eventStore
        .subscribe<StoredEvent>(
          const SubscriptionFilter(
            aggregateTypes: {'role_permission_grant', 'user_role_scope'},
          ),
          const Events(),
        )
        .listen(_handleCoreEvent);
  }

  /// Opt-in: also watch a containment projection's event source.
  /// When the projection's view-rows change, emit stale_data to all
  /// currently-connected users.
  void watchContainment(String aggregateType) {
    final sub = eventStore
        .subscribe<StoredEvent>(
          SubscriptionFilter(aggregateTypes: {aggregateType}),
          const Events(),
        )
        .listen(_handleContainmentEvent);
    _containmentSubs.add(sub);
  }

  Future<void> stop() async {
    await _coreSub?.cancel();
    for (final s in _containmentSubs) {
      await s.cancel();
    }
  }

  void _handleCoreEvent(Update<StoredEvent> update) {
    if (update is! Delta<StoredEvent>) return;
    final event = update.value;
    switch (event.eventType) {
      case 'role_unassigned':
        _forceLogout(event.data['user_id']! as String);
      case 'role_assigned':
        _staleData(
          event.data['user_id']! as String,
          StaleDataReason.roleAssigned,
        );
      case 'permission_revoked':
        unawaited(_forceLogoutAllWithRole(event.data['role']! as String));
      case 'permission_granted':
        unawaited(
          _staleDataAllWithRole(
            event.data['role']! as String,
            StaleDataReason.permissionAdded,
          ),
        );
    }
  }

  void _handleContainmentEvent(Update<StoredEvent> update) {
    if (update is! Delta<StoredEvent>) return;
    // Emit stale_data to ALL connected users — coarse fan-out.
    for (final uid in connectionRegistry.connectedUserIds.toList()) {
      _staleData(uid, StaleDataReason.containmentChanged);
    }
  }

  void _forceLogout(String userId) {
    for (final ch in connectionRegistry.channelsFor(userId).toList()) {
      ch.sink.close(4003, 'permissions_changed');
    }
  }

  void _staleData(String userId, StaleDataReason reason) {
    final envelope = jsonEncode(
      SubscriptionMessages.encodeServer(StaleDataMsg(reason: reason)),
    );
    for (final ch in connectionRegistry.channelsFor(userId)) {
      ch.sink.add(envelope);
    }
  }

  Future<void> _forceLogoutAllWithRole(String role) async {
    for (final uid in connectionRegistry.connectedUserIds.toList()) {
      // Synthetic principal to query the policy (activeRole is not
      // cached per-connection; this reconstructs it from the role name).
      final principal = UserPrincipal(
        userId: uid,
        roles: {role},
        activeRole: role,
      );
      final eff = await policy.effectivePermissionsFor(principal);
      if (eff.activeRole == role) _forceLogout(uid);
    }
  }

  Future<void> _staleDataAllWithRole(
    String role,
    StaleDataReason reason,
  ) async {
    for (final uid in connectionRegistry.connectedUserIds.toList()) {
      final principal = UserPrincipal(
        userId: uid,
        roles: {role},
        activeRole: role,
      );
      final eff = await policy.effectivePermissionsFor(principal);
      if (eff.activeRole == role) _staleData(uid, reason);
    }
  }
}
