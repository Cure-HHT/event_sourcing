// WS subscription state machine (AWAITING_AUTH -> AUTHENTICATED),
// per-subscribe two-tier authorization (Approach B), and per-connection
// serialized relay of substrate Update envelopes.
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
  ScopeClassRegistry? scopeClassRegistry,
}) {
  _ConnectionState(
    channel: channel,
    validator: validator,
    eventStore: eventStore,
    policy: policy,
    viewScopes: viewScopes,
    viewPermissionNamer: viewPermissionNamer,
    connectionRegistry: connectionRegistry,
    scopeClassRegistry: scopeClassRegistry,
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
    required this.scopeClassRegistry,
  });

  final WebSocketChannel channel;
  final PrincipalAuthValidator validator;
  final EventStore eventStore;
  final AuthorizationPolicy policy;
  final ViewScopeRegistry viewScopes;
  final ViewPermissionNamer viewPermissionNamer;
  final WsConnectionRegistry connectionRegistry;

  /// Scope-class hierarchy used to mirror the write-path policy's
  /// class-and-ancestry matching when narrowing rows. When `null`,
  /// `_expandAssignments` falls back to exact-class matching only
  /// (conservative-correct: under-grants the ancestor-wildcard case,
  /// never over-grants).
  final ScopeClassRegistry? scopeClassRegistry;

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
  /// (no row-level narrowing required).
  ///
  /// The class gate (equals-or-ancestor) is computed by the shared
  /// `scopeClassMatch` helper in event_sourcing — the single tested source
  /// of truth that the write-path policy's `_matches`
  /// (`TableBackedAuthorizationPolicy`) also obeys, so the read path cannot
  /// over-grant relative to the write path by construction.
  /// Each assignment is checked against the view binding's [scopeClass]
  /// BEFORE it is consumed:
  ///
  /// - `TotalWildcardScope` → genuinely unrestricted (no class). Returns
  ///   `null`.
  /// - `ValueWildcardScope(class_: c)` → unrestricted ONLY IF `c` equals
  ///   the view's scope class OR `c` is an ancestor of it (per the
  ///   [scopeClassRegistry]). An unrelated class (e.g. a 'site' wildcard
  ///   on a 'patient' view) is skipped, NOT treated as unrestricted.
  /// - `BoundScope(class_: c, value: v)` → resolved through the binding's
  ///   `aggregateIdResolver` ONLY on an exact class match (`c == scope
  ///   class`). Resolver-returns-null entries are skipped (direct
  ///   translation unavailable).
  ///
  /// When [scopeClassRegistry] is `null`, the ancestor check is skipped:
  /// only exact-class `ValueWildcardScope` grants unrestricted. This is
  /// conservative-correct (under-grants the legitimate ancestor case,
  /// never over-grants).
  ///
  /// Ancestor `BoundScope`s (e.g. `BoundScope('site', 'site-A')` on a
  /// 'patient' view) require `ContainmentResolver` expansion to enumerate
  /// "patients at site-A"; that plumbing is deferred to a follow-up task,
  /// so such assignments are conservatively SKIPPED here (under-grant,
  /// never over-grant). This is intentional, not a bug.
  Set<String>? _expandAssignments({
    required List<ScopeAssignment> assignments,
    required ViewScopeBinding binding,
  }) {
    final registry = scopeClassRegistry;
    final result = <String>{};
    for (final a in assignments) {
      final scope = a.scope;

      // A total wildcard carries no class: genuinely unrestricted, before
      // the class gate even comes into play.
      if (scope is TotalWildcardScope) {
        return null;
      }

      // The class gate (equals-or-ancestor of the view's scope class) is the
      // single tested source of truth shared with the write path; see
      // scopeClassMatch in event_sourcing. When the registry is null we
      // cannot compute ancestry, so we fall back to exact-class-only matching
      // (conservative: under-grants the ancestor case, never over-grants —
      // see the doc comment above).
      final ScopeClassMatch match;
      if (registry == null) {
        // No registry → ancestry is unknowable. Exact-class match only;
        // everything else is conservatively does-not-apply.
        final scopeClass = switch (scope) {
          TotalWildcardScope() => null, // unreachable; handled above
          ValueWildcardScope(class_: final c) => c,
          BoundScope(class_: final c) => c,
        };
        match = scopeClass == binding.scopeClass
            ? ScopeClassMatch.appliesExact
            : ScopeClassMatch.doesNotApply;
      } else {
        match = scopeClassMatch(scope, binding.scopeClass, registry);
      }

      switch (match) {
        case ScopeClassMatch.doesNotApply:
          // Unrelated class (e.g. a 'site' wildcard on a 'patient' view, or a
          // cross-class bound scope): grants nothing here. Keep scanning.
          break;
        case ScopeClassMatch.appliesExact:
          switch (scope) {
            case TotalWildcardScope():
              // Unreachable (handled above), but keeps the switch exhaustive.
              return null;
            case ValueWildcardScope():
              // Covers every aggregate in this class: no narrowing applies.
              return null;
            case BoundScope():
              final aggId = binding.aggregateIdResolver(scope);
              if (aggId != null) {
                result.add(aggId);
              }
            // null = resolver couldn't directly translate; needs the
            // containment resolver, wired in a follow-up.
          }
        case ScopeClassMatch.appliesViaAncestor:
          switch (scope) {
            case TotalWildcardScope():
              // Unreachable (handled above); keeps the switch exhaustive.
              return null;
            case ValueWildcardScope():
              // Ancestor wildcard covers the whole descendant class: no
              // narrowing applies. (Preserves prior read-path behavior.)
              return null;
            case BoundScope():
              // An ancestor BoundScope (e.g. BoundScope('site', 'site-A') on
              // a 'patient' view) needs ContainmentResolver expansion to
              // enumerate the descendants; that plumbing arrives in a
              // follow-up, so skip conservatively (under-grant, never over).
              break;
          }
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
