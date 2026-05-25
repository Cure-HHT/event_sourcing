// Implements: EVS-PRD-subscription remote-server side — WS subscription
// state machine (AWAITING_AUTH -> AUTHENTICATED), per-subscribe two-tier
// authorization (Approach B), and per-connection serialized relay of
// substrate Update envelopes.
//
// Package-private; not exported from reaction.dart.
//
// Implements: EVS-PRD-cross-process-event-transport/C — serializes
//   per-connection writes onto the WS sink so per-subscription ordering
//   end-to-end matches a co-located in-process subscriber.
// Implements: EVS-PRD-cross-process-event-transport/D — multiplexes
//   multiple substrate subscriptions over one WS connection, keyed by
//   client-chosen subscriptionId.
// Implements: EVS-PRD-cross-process-event-transport/E — per-subscription
//   authorization: view-level deny via policy.isPermitted, then row-
//   level narrowing via effectivePermissionsFor expanded through the
//   ViewScopeRegistry binding before opening the substrate subscribe<T>.
// Implements: EVS-PRD-auth-session/C — invokes the supplied
//   PrincipalAuthValidator on the first WS message's credential.
// Implements: EVS-PRD-auth-session/E — close-frame 4001 auth_rejected
//   on validator failure is the wire signal RemoteAuthSession maps to
//   AuthStatus.Expired.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:reaction/src/wire/subscription_messages.dart';
import 'package:reaction/src/wire/update_codec.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Composition-time mapper from `ProjectionSpec.viewName` to the
/// view-level `Permission` name required to subscribe to it.
///
/// Returning `null` skips the view-level check entirely — used for
/// public / admin-default views that gate row-level only (or not at
/// all). Returning a non-null name causes the handler to call
/// `policy.isPermitted(principal, Permission(name), null)` and to
/// reject the subscription on `Deny`.
typedef ViewPermissionNamer = String? Function(String viewName);

/// Run the per-connection state machine for an already-upgraded
/// WebSocket [channel]. Spawns a message loop that:
///
///   1. Waits for [AuthMsg] as the first wire message.
///      - On success: emits [AuthOkMsg] and transitions to
///        AUTHENTICATED.
///      - On failure: closes the WS with code 4001 ("auth_rejected").
///   2. While AUTHENTICATED, processes [SubscribeMsg] and
///      [UnsubscribeMsg]. Re-auth is rejected as a protocol error in
///      v1.
///   3. For each [SubscribeMsg], applies two-tier authorization
///      (Approach B):
///        a. View-level: `policy.isPermitted` against the named
///           view permission. `Deny` → emits
///           [SubscriptionDeniedMsg].
///        b. Row-level: expands the principal's scope assignments
///           through the registered [ViewScopeRegistry] binding into
///           the set of allowed aggregate IDs; intersects with any
///           client-supplied `aggregates:` allow-list.
///   4. Opens an [EventStore.subscribe] in [AggregateMode] and
///      relays [Update] envelopes via [UpdateCodec.encode].
///   5. Serializes all outbound writes through a single async write
///      chain to guarantee per-connection wire ordering.
///   6. Cleans up subscriptions on stream close or error.
///
/// All work happens asynchronously; this returns immediately after
/// attaching the stream listener.
void runSubscriptionHandler({
  required WebSocketChannel channel,
  required PrincipalAuthValidator validator,
  required EventStore eventStore,
  required AuthorizationPolicy policy,
  required ViewScopeRegistry viewScopes,
  required ViewPermissionNamer viewPermissionNamer,
  required WsConnectionRegistry connectionRegistry,
}) {
  _ConnectionState(
    channel: channel,
    validator: validator,
    eventStore: eventStore,
    policy: policy,
    viewScopes: viewScopes,
    viewPermissionNamer: viewPermissionNamer,
    connectionRegistry: connectionRegistry,
  ).start();
}

class _ConnectionState {
  _ConnectionState({
    required this.channel,
    required this.validator,
    required this.eventStore,
    required this.policy,
    required this.viewScopes,
    required this.viewPermissionNamer,
    required this.connectionRegistry,
  });

  final WebSocketChannel channel;
  final PrincipalAuthValidator validator;
  final EventStore eventStore;
  final AuthorizationPolicy policy;
  final ViewScopeRegistry viewScopes;
  final ViewPermissionNamer viewPermissionNamer;
  final WsConnectionRegistry connectionRegistry;

  Principal? _principal;
  final Map<String, StreamSubscription<Update<Map<String, Object?>>>> _subs =
      <String, StreamSubscription<Update<Map<String, Object?>>>>{};

  // Single async write chain ensures envelopes ship in submission
  // order across all subscriptions on this connection.
  Future<void> _writeChain = Future<void>.value();

  void start() {
    channel.stream.listen(
      _onMessage,
      onDone: _cleanup,
      onError: (Object _) => _cleanup(),
      cancelOnError: false,
    );
  }

  /// Enqueue an outbound server envelope. Encodes inline (cheap) but
  /// serializes the `channel.sink.add` through `_writeChain` so that
  /// concurrent producers (multiple substrate subscriptions) cannot
  /// interleave on the wire.
  void _send(Map<String, Object?> envelope) {
    final encoded = jsonEncode(envelope);
    _writeChain = _writeChain.then((_) async {
      channel.sink.add(encoded);
    });
  }

  Future<void> _onMessage(dynamic raw) async {
    final Map<String, Object?> json;
    try {
      json = jsonDecode(raw as String) as Map<String, Object?>;
    } on FormatException {
      _send(
        SubscriptionMessages.encodeServer(
          const ErrorMsg(
            code: WireErrorCode.protocolError,
            message: 'malformed json',
          ),
        ),
      );
      return;
    }

    if (_principal == null) {
      await _handleAwaitingAuth(json);
    } else {
      await _handleAuthenticated(json);
    }
  }

  Future<void> _handleAwaitingAuth(Map<String, Object?> json) async {
    final ClientMessage msg;
    try {
      msg = SubscriptionMessages.decodeClient(json);
    } on FormatException {
      await channel.sink.close(4001, 'auth_rejected');
      return;
    }
    if (msg is! AuthMsg) {
      await channel.sink.close(4001, 'auth_rejected');
      return;
    }
    try {
      final principal = await validator.authenticate(msg.credential);
      _principal = principal;
      final principalId = principal is UserPrincipal
          ? principal.userId
          : principal.id;
      connectionRegistry.register(principalId, channel);
      _send(
        SubscriptionMessages.encodeServer(AuthOkMsg(principalId: principalId)),
      );
    } on AuthenticationDenied {
      await channel.sink.close(4001, 'auth_rejected');
    }
  }

  Future<void> _handleAuthenticated(Map<String, Object?> json) async {
    final ClientMessage msg;
    try {
      msg = SubscriptionMessages.decodeClient(json);
    } on FormatException catch (e) {
      _send(
        SubscriptionMessages.encodeServer(
          ErrorMsg(code: WireErrorCode.protocolError, message: e.message),
        ),
      );
      return;
    }

    if (msg is SubscribeMsg) {
      await _handleSubscribe(msg);
    } else if (msg is UnsubscribeMsg) {
      final removed = _subs.remove(msg.subscriptionId);
      if (removed != null) {
        await removed.cancel();
      }
    } else if (msg is AuthMsg) {
      // Re-auth in v1 is not supported; surface as a protocol error
      // so a buggy client sees something other than silence.
      _send(
        SubscriptionMessages.encodeServer(
          const ErrorMsg(
            code: WireErrorCode.protocolError,
            message: 're-auth not supported',
          ),
        ),
      );
    }
  }

  Future<void> _handleSubscribe(SubscribeMsg msg) async {
    final principal = _principal!;

    // --- Step 1: view-level permission gate. ---
    final required = viewPermissionNamer(msg.viewName);
    if (required != null) {
      final decision = await policy.isPermitted(
        principal,
        Permission(required),
        null,
      );
      if (decision is! Allow) {
        _send(
          SubscriptionMessages.encodeServer(
            SubscriptionDeniedMsg(
              subscriptionId: msg.subscriptionId,
              reason: SubscriptionDenyReason.viewPermissionDenied,
            ),
          ),
        );
        return;
      }
    }

    // --- Step 2: row-level narrowing via the view's scope binding. ---
    Set<String>? allowedAggregates;
    final binding = viewScopes.lookup(msg.viewName);
    if (binding != null) {
      final eff = await policy.effectivePermissionsFor(principal);
      allowedAggregates = _expandAssignments(
        assignments: eff.scopeAssignments,
        binding: binding,
      );
    }

    // --- Step 3: intersect any client-supplied aggregates allow-list
    //             with the row-level allow set. ---
    Set<String>? effectiveAggregates;
    if (msg.aggregates == null) {
      effectiveAggregates = allowedAggregates;
    } else if (allowedAggregates == null) {
      effectiveAggregates = msg.aggregates;
    } else {
      effectiveAggregates = msg.aggregates!.intersection(allowedAggregates);
    }

    // --- Step 4: open the substrate subscription and relay. ---
    final stream = eventStore.subscribe<Map<String, Object?>>(
      msg.filter ?? const SubscriptionFilter(),
      AggregateMode<Map<String, Object?>>(
        viewName: msg.viewName,
        mapper: (row) => row,
        aggregates: effectiveAggregates,
      ),
    );
    // Stored in `_subs` and cancelled by `_cleanup` or by an explicit
    // UnsubscribeMsg; the lint can't see across the map indirection.
    // ignore: cancel_subscriptions
    final sub = stream.listen((update) {
      _send(UpdateCodec.encode(update, subscriptionId: msg.subscriptionId));
    });
    _subs[msg.subscriptionId] = sub;
  }

  /// Expand the principal's scope assignments through [binding] into
  /// the set of aggregate IDs covered. Returns `null` for "unrestricted"
  /// (no row-level narrowing required) when any assignment is a
  /// `TotalWildcardScope` or `ValueWildcardScope` against the view's
  /// scope class.
  ///
  /// `BoundScope` assignments are passed through the binding's
  /// `aggregateIdResolver`; resolver-returns-null entries are treated
  /// as "needs containment expansion" and silently skipped here —
  /// `ContainmentResolver` plumbing arrives in a follow-up task.
  Set<String>? _expandAssignments({
    required List<ScopeAssignment> assignments,
    required ViewScopeBinding binding,
  }) {
    if (assignments.any((a) => a.scope is TotalWildcardScope)) {
      return null;
    }
    final result = <String>{};
    for (final a in assignments) {
      final scope = a.scope;
      if (scope is BoundScope) {
        final aggId = binding.aggregateIdResolver(scope);
        if (aggId != null) {
          result.add(aggId);
        }
        // null = resolver couldn't directly translate; needs the
        // containment resolver, wired in a follow-up.
      } else if (scope is ValueWildcardScope) {
        // Wildcard over this scope class = no row-level narrowing
        // applicable for this assignment (the user covers every
        // aggregate in the class).
        return null;
      }
    }
    return result;
  }

  Future<void> _cleanup() async {
    for (final sub in _subs.values) {
      await sub.cancel();
    }
    _subs.clear();
    final principal = _principal;
    if (principal != null) {
      final principalId = principal is UserPrincipal
          ? principal.userId
          : principal.id;
      connectionRegistry.unregister(principalId, channel);
    }
  }
}
