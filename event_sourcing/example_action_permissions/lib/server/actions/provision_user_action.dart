// IMPLEMENTS REQUIREMENTS:
//
// Admin-only. Validates uniqueness against the current in-memory directory
// view. Emits a user_provisioned event (projected by the
// UserDirectoryMaterializer subscriber back into the in-memory directory)
// and — when activeSite is non-null — a role_assigned event so the
// user_role_scopes view materializes the new user's site-scoped
// assignment atomically inside the dispatch transaction.
//
// The role_assigned event is emitted by the action itself (not by a
// post-commit subscriber bridge): EventStore.appendInTxn runs the
// projection interpreter inside the dispatch transaction, so the new
// user's scope assignment is visible to the very next action that reads
// user_role_scopes.

import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
class ProvisionUserInput {
  const ProvisionUserInput({
    required this.userId,
    required this.role,
    required this.activeSite,
  });

  final String userId;
  final String role;
  final String? activeSite;
}

@immutable
class ProvisionUserResult {
  const ProvisionUserResult({required this.userId});

  final String userId;

  Map<String, Object?> toJson() => <String, Object?>{'userId': userId};
}

class ProvisionUserAction
    extends Action<ProvisionUserInput, ProvisionUserResult> {
  ProvisionUserAction({required this.directory});

  final UserDirectory directory;

  @override
  String get name => 'ProvisionUserAction';

  @override
  String get description =>
      'Admin provisions a new user; emits a user_provisioned event '
      'that the directory materializer projects into the in-memory map, '
      'and (when activeSite is non-null) a role_assigned event that '
      'populates the user_role_scopes view inside the dispatch tx.';

  @override
  Set<Permission> get permissions => <Permission>{
    const Permission('users.provision'),
  };

  @override
  Idempotency get idempotency => Idempotency.required;

  @override
  ProvisionUserInput parseInput(Map<String, Object?> raw) {
    final userId = raw['userId'];
    final role = raw['role'];
    final activeSite = raw['activeSite'];
    if (userId is! String || role is! String) {
      throw const FormatException(
        'ProvisionUserAction expects {userId, role}: String, '
        'optional {activeSite}: String?',
      );
    }
    if (activeSite != null && activeSite is! String) {
      throw const FormatException(
        'ProvisionUserAction "activeSite" must be String or null',
      );
    }
    return ProvisionUserInput(
      userId: userId,
      role: role,
      activeSite: activeSite as String?,
    );
  }

  @override
  void validate(ProvisionUserInput input) {
    if (input.userId.trim().isEmpty) {
      throw ArgumentError.value(input.userId, 'userId', 'must be non-empty');
    }
    if (input.role.trim().isEmpty) {
      throw ArgumentError.value(input.role, 'role', 'must be non-empty');
    }
    if (directory.contains(input.userId)) {
      throw ArgumentError.value(input.userId, 'userId', 'already provisioned');
    }
  }

  @override
  Future<ExecutionResult<ProvisionUserResult>> execute(
    ProvisionUserInput input,
    ActionContext ctx,
  ) async {
    final events = <EventDraft>[
      EventDraft(
        aggregateType: 'user_directory',
        aggregateId: input.userId,
        entryType: 'user_provisioned',
        eventType: 'user_provisioned',
        data: <String, dynamic>{
          'userId': input.userId,
          'role': input.role,
          'activeSite': input.activeSite,
        },
      ),
    ];

    // When activeSite is non-null, emit a role_assigned event so the
    // user_role_scopes view materializes the new user's site-scoped
    // assignment inside the dispatch transaction. EventStore.appendInTxn
    // runs the projection interpreter atomically with the append, so the
    // very next dispatch reading user_role_scopes sees the new row.
    final activeSite = input.activeSite;
    if (activeSite != null && activeSite.isNotEmpty) {
      final scope = BoundScope(class_: 'site', value: activeSite);
      final aggregateId = roleAssignmentAggregateId(
        userId: input.userId,
        role: input.role,
        scope: scope,
      );
      events.add(
        EventDraft(
          aggregateType: 'user_role_scope',
          aggregateId: aggregateId,
          entryType: 'user_role_scope',
          eventType: 'role_assigned',
          data: RoleAssignedPayload(
            userId: input.userId,
            role: input.role,
            scope: scope,
          ).toJson(),
        ),
      );
    }

    return ExecutionResult<ProvisionUserResult>(
      result: ProvisionUserResult(userId: input.userId),
      events: events,
    );
  }
}
