// Test fixtures: concrete Action and AuthorizationPolicy implementations used
// across the actions test suite.
// Verifies: EVS-PRD-action-dispatch/A (concrete Action subclasses: HelloAction, BadParseAction, BadValidateAction, BadExecuteAction, OptionalKeyAction, RequiredKeyAction, TwoPermissionAction, MultiEventAction)
// Verifies: EVS-PRD-action-dispatch/B (AlwaysAllowPolicy satisfies AuthorizationPolicy interface for all-Allow path testing)

import 'package:event_sourcing/event_sourcing.dart' show EventDraft;
import 'package:event_sourcing/src/actions/action.dart';
import 'package:event_sourcing/src/actions/action_context.dart';
import 'package:event_sourcing/src/actions/authorization_decision.dart'
    show Allow, AuthorizationDecision, Deny, DenyReason;
import 'package:event_sourcing/src/actions/authorization_policy.dart'
    show AuthorizationPolicy;
import 'package:event_sourcing/src/actions/execution_result.dart';
import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart' show Principal;
import 'package:event_sourcing/src/actions/scope_value.dart'
    show BoundScope, ScopeValue;
import 'package:event_sourcing/src/permissions/effective_authorization.dart'
    show EffectiveAuthorization;
import 'package:event_sourcing/src/storage/txn.dart' show Txn;

/// Always-succeeds, emits one event.
class HelloAction extends Action<Map<String, Object?>, String> {
  @override
  String get name => 'hello';

  @override
  String get description => 'Say hello.';

  @override
  Set<Permission> get permissions => {const Permission('test.hello')};

  @override
  Idempotency get idempotency => Idempotency.none;

  @override
  Map<String, Object?> parseInput(Map<String, Object?> raw) =>
      <String, Object?>{'who': raw['who'] as String};

  @override
  void validate(Map<String, Object?> input) {}

  @override
  Future<ExecutionResult<String>> execute(
    Map<String, Object?> input,
    ActionContext ctx,
  ) async {
    return ExecutionResult<String>(
      result: 'Hello, ${input['who']}',
      events: <EventDraft>[
        EventDraft(
          aggregateId: 'greeting-${input['who']}',
          aggregateType: 'greeting',
          entryType: 'greeting',
          eventType: 'hello.said',
          data: <String, dynamic>{'who': input['who']},
        ),
      ],
    );
  }
}

/// Throws on parse.
class BadParseAction extends HelloAction {
  @override
  String get name => 'bad_parse';

  @override
  Map<String, Object?> parseInput(Map<String, Object?> raw) {
    throw ArgumentError('parse failure');
  }
}

/// Throws on validate.
class BadValidateAction extends HelloAction {
  @override
  String get name => 'bad_validate';

  @override
  void validate(Map<String, Object?> input) {
    throw StateError('validate failure');
  }
}

/// Throws on execute.
class BadExecuteAction extends HelloAction {
  @override
  String get name => 'bad_execute';

  @override
  Future<ExecutionResult<String>> execute(
    Map<String, Object?> input,
    ActionContext ctx,
  ) async {
    throw StateError('execute failure');
  }
}

/// Idempotency.optional action — key is accepted but not required.
class OptionalKeyAction extends HelloAction {
  @override
  String get name => 'optional_key';

  @override
  Idempotency get idempotency => Idempotency.optional;
}

/// Idempotency.required action.
class RequiredKeyAction extends HelloAction {
  @override
  String get name => 'requires_key';

  @override
  Idempotency get idempotency => Idempotency.required;
}

/// Action with TWO permissions, for testing first-deny short-circuit.
class TwoPermissionAction extends HelloAction {
  @override
  String get name => 'two_perms';

  @override
  Set<Permission> get permissions => {
    const Permission('test.first'),
    const Permission('test.second'),
  };
}

/// Emits 3 events on execute. Used in Stage 8 atomic-persist tests.
class MultiEventAction extends HelloAction {
  @override
  String get name => 'multi_event';

  @override
  Future<ExecutionResult<String>> execute(
    Map<String, Object?> input,
    ActionContext ctx,
  ) async {
    return const ExecutionResult<String>(
      result: 'multi',
      events: <EventDraft>[
        EventDraft(
          aggregateId: 'multi-agg',
          aggregateType: 'greeting',
          entryType: 'greeting',
          eventType: 'hello.said',
          data: <String, dynamic>{'who': 'alpha'},
        ),
        EventDraft(
          aggregateId: 'multi-agg',
          aggregateType: 'greeting',
          entryType: 'greeting',
          eventType: 'hello.said',
          data: <String, dynamic>{'who': 'beta'},
        ),
        EventDraft(
          aggregateId: 'multi-agg',
          aggregateType: 'greeting',
          entryType: 'greeting',
          eventType: 'hello.said',
          data: <String, dynamic>{'who': 'gamma'},
        ),
      ],
    );
  }
}

/// Authorization policy that allows every request. Used in Stage 6 tests
/// to verify the all-Allow path falls through to Stage 7.
class AlwaysAllowPolicy extends AuthorizationPolicy {
  const AlwaysAllowPolicy();

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async => const Allow();

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async => EffectiveAuthorization.empty;
}

/// Records every (permission, scopeValue, txn) tuple the dispatcher
/// passes to [isPermitted]; always allows. Used to verify the dispatcher
/// (a) hands the resolved scope through to the policy and (b) injects
/// the active dispatch transaction so authorize + execute share a
/// read-snapshot.
class RecordingAllowPolicy extends AuthorizationPolicy {
  RecordingAllowPolicy();

  final List<(Permission, ScopeValue?)> calls = <(Permission, ScopeValue?)>[];

  /// Every [Txn] the dispatcher injected into [isPermitted], in call
  /// order. Tests assert non-null and identity-equality with the txn the
  /// dispatcher's [EventStore.runTransaction] body received.
  final List<Txn?> txns = <Txn?>[];

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async {
    calls.add((permission, scopeValue));
    txns.add(txn);
    return const Allow();
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async => EffectiveAuthorization.empty;
}

/// Policy that always denies with [DenyReason.notGranted]. Used to verify
/// the dispatcher stamps the resolved scope onto authorization_denied
/// events emitted from the policy-level deny path.
class AlwaysDenyNotGrantedPolicy extends AuthorizationPolicy {
  const AlwaysDenyNotGrantedPolicy();

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async => Deny(permission: permission, reason: DenyReason.notGranted);

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async => EffectiveAuthorization.empty;
}

/// Action requiring a scoped permission `patient.edit` (scopeClass:
/// `patient`). `scopeFor` returns a configurable `ScopeValue`; defaults to
/// a `BoundScope` of class `patient` matching `raw['patient_id']`.
///
/// Exercises the dispatcher's scope-resolution branch across:
///   - returning a class-matching BoundScope (Allow path)
///   - returning null (Deny scopeUnresolvable: missing scope)
///   - returning TotalWildcardScope (Deny scopeUnresolvable: no class)
///   - returning a BoundScope with a mismatched class (Deny scopeUnresolvable)
class ScopedPatientEditAction extends Action<Map<String, Object?>, String> {
  const ScopedPatientEditAction({this.scopeOverride, this.forceNull = false});

  /// If non-null AND [forceNull] is false, overrides scopeFor's return value.
  final ScopeValue? scopeOverride;

  /// If true, scopeFor returns null regardless of [scopeOverride] (Dart
  /// cannot distinguish "no override" from "override with null" with a
  /// single nullable field, so this boolean disambiguates the two cases).
  final bool forceNull;

  @override
  String get name => 'patient_edit';

  @override
  String get description => 'Edit a patient (scoped).';

  @override
  Set<Permission> get permissions => {
    const Permission('patient.edit', scopeClass: 'patient'),
  };

  @override
  Idempotency get idempotency => Idempotency.none;

  @override
  Map<String, Object?> parseInput(Map<String, Object?> raw) =>
      <String, Object?>{'patient_id': raw['patient_id'] as String};

  @override
  void validate(Map<String, Object?> input) {}

  @override
  Future<ExecutionResult<String>> execute(
    Map<String, Object?> input,
    ActionContext ctx,
  ) async {
    return ExecutionResult<String>(
      result: 'edited ${input['patient_id']}',
      events: <EventDraft>[
        // Reuses the `greeting` entry type registered by
        // bootstrapTestEventStore so tests don't need extra fixture wiring.
        EventDraft(
          aggregateId: 'patient-${input['patient_id']}',
          aggregateType: 'patient',
          entryType: 'greeting',
          eventType: 'patient.edited',
          data: <String, dynamic>{'patient_id': input['patient_id']},
        ),
      ],
    );
  }

  @override
  ScopeValue? scopeFor(Permission perm, Map<String, Object?> input) {
    if (forceNull) return null;
    final override = scopeOverride;
    if (override != null) return override;
    return BoundScope(class_: 'patient', value: input['patient_id']! as String);
  }
}
