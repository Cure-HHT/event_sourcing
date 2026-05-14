// lib/server/inspect_snapshot.dart
// IMPLEMENTS REQUIREMENTS:
//   inspector snapshot.
//
// Pure-ish helpers: each takes a backend / directory / store and emits a
// list of wire-shape summaries. No HTTP, no JSON encoding here — that
// happens at the route boundary.

import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';
import 'package:event_sourcing/event_sourcing.dart';

Future<List<StoredEventSummary>> collectEventSummaries(
  EventStore store,
  UserDirectory directory, {
  required int limit,
}) async {
  final events = await store.backend.findAllEvents(limit: limit);
  return events.map((e) {
    final initiator = e.initiator;
    final initiatorUserId = switch (initiator) {
      UserInitiator() => initiator.userId,
      AutomationInitiator() => null,
      AnonymousInitiator() => null,
    };
    final initiatorRole = switch (initiator) {
      UserInitiator() => _roleFor(initiator.userId, directory),
      AutomationInitiator() => 'automation:${initiator.service}',
      AnonymousInitiator() => 'anonymous',
    };
    return StoredEventSummary(
      eventId: e.eventId,
      eventType: e.eventType,
      aggregateType: e.aggregateType,
      aggregateId: e.aggregateId,
      actionInvocationId: (e.metadata['action_invocation_id'] as String?) ?? '',
      initiatorUserId: initiatorUserId,
      initiatorRole: initiatorRole,
    );
  }).toList();
}

String _roleFor(String userId, UserDirectory directory) {
  for (final entry in directory.listEntries()) {
    if (entry.userId == userId) return entry.role;
  }
  return 'unknown';
}

Future<List<MatrixGrant>> collectMatrixGrants(EventStore store) async {
  final rows = await store.backend.findViewRows('role_permission_grants');
  return rows
      .map(
        (r) => MatrixGrant(
          role: r['role']! as String,
          permission: r['permissionName']! as String,
        ),
      )
      .toList()
    ..sort((a, b) {
      final r = a.role.compareTo(b.role);
      return r != 0 ? r : a.permission.compareTo(b.permission);
    });
}

/// Inspector-only enumeration of cached idempotency entries.
///
/// Goes through the `IdempotencyStore.listEntries()` contract, which
/// every backend implements (in-memory, demo in-memory, postgres). The
/// inspector pane renders the keying tuple and expiry; cache payloads
/// (`resultJson`, `emittedEventIds`) are deliberately omitted from
/// [IdempotencyEntrySummary] to keep the inspector lightweight and avoid
/// exposing the action's outcome shape over the wire.
Future<List<IdempotencyEntrySummary>> collectIdempotencyEntries(
  IdempotencyStore store,
) async {
  final entries = await store.listEntries();
  return entries
      .map(
        (e) => IdempotencyEntrySummary(
          actionName: e.actionName,
          principalUserId: e.principalId,
          idempotencyKey: e.idempotencyKey,
          expiresAt: e.expiresAt,
        ),
      )
      .toList(growable: false);
}
