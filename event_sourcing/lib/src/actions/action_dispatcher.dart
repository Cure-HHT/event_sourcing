// Implements: EVS-PRD-action-dispatch/A (accepts actions submitted by principals)
// Implements: EVS-PRD-action-dispatch/B (parse → validate → authorize → execute → record, in that order)
// Implements: EVS-PRD-action-dispatch/C (every dispatch produces a recorded outcome: success events or denial event)
// Implements: EVS-PRD-action-dispatch/D (idempotency: same identifier + matching content → same outcome, no new event)
// Implements: EVS-PRD-action-dispatch/E (Stage 4 compares the submitted rawInput's canonical JSON against the cached entry's rawInputCanonicalJson; mismatch emits idempotency_mismatch denial and returns DispatchIdempotencyMismatch)
// Implements: EVS-DEV-flow-token/B - threads flowToken onto every appended event (Stage 8) and onto every denial event via _persistDenial.
// Implements: EVS-PRD-action-dispatch/F (single path by which consumer-initiated events enter the log)
// Implements: EVS-PRD-library-charter/C (authorization-checked dispatch; decision and state change both recorded)
// Implements: EVS-PRD-scoped-permissions/E/H/I — dispatcher resolves the
//   per-permission scope, wraps authorize+execute+persist in one storage
//   transaction, and stamps the resolved scope onto authorization_denied
//   events when it was non-null.
// Implements: EVS-DEV-transactional-authorize-execute — single dispatch
//   tx covers Stage 6 policy reads, Stage 7 execute, and Stage 8 appends;
//   authorize-stage denials commit inside the tx; execute / append throws
//   roll back and emit execution_failed in a separate append.
// Implements: EVS-DEV-scope-unresolvable-denial — pre-tx invocation of
//   Action.scopeFor, denial with scopeUnresolvable for null /
//   TotalWildcardScope / class-mismatched returns, and scope stamping
//   onto the denial event when one was returned.

import 'dart:convert' show utf8;

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart' show sha256;
import 'package:event_sourcing/src/actions/action_context.dart';
import 'package:event_sourcing/src/actions/action_registry.dart';
import 'package:event_sourcing/src/actions/action_submission.dart';
import 'package:event_sourcing/src/actions/authorization_decision.dart'
    show Deny, DenyReason;
import 'package:event_sourcing/src/actions/authorization_policy.dart';
import 'package:event_sourcing/src/actions/denial_events.dart';
import 'package:event_sourcing/src/actions/dispatch_result.dart';
import 'package:event_sourcing/src/actions/execution_result.dart';
import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:event_sourcing/src/actions/idempotency_errors.dart';
import 'package:event_sourcing/src/actions/idempotency_store.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart' show UserPrincipal;
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:uuid/uuid.dart';

/// Runs every untrusted-ingress action through the standard 10-stage
/// pipeline. in `spec/dev-event-sourcing.md` for the
/// stage list and contract.
class ActionDispatcher {
  ActionDispatcher({
    required this.registry,
    required this.authorization,
    required this.events,
    required this.idempotency,
  });

  final ActionRegistry registry;
  final AuthorizationPolicy authorization;
  final EventStore events;
  final IdempotencyStore idempotency;

  static const _uuid = Uuid();

  /// Dispatch one action call.
  ///
  /// Pipeline (per ):
  ///   Stage 1 — lookup `actionName` in [registry]. If unknown, emit
  ///             `unknown_action` denial event and return
  ///             [DispatchUnknownAction].
  ///   Stage 2 — generate v4 UUID `action_invocation_id`; stamp it into
  ///             every emitted event's metadata.
  ///   (Pre-Stage 3) — idempotency-required precondition check
  ///            : if action requires a key and none was
  ///             supplied, emit `parse_denied` and return before
  ///             parseInput runs.
  ///   Stage 3 — call `action.parseInput(rawInput)`; on throw, emit
  ///             `parse_denied` and return [DispatchParseDenied].
  ///   Stage 4 — idempotency cache lookup for non-none policies with a
  ///             key. On hit, compare the submitted rawInput's canonical
  ///             JSON (RFC 8785) against the cached
  ///             `rawInputCanonicalJson`:
  ///               - match (or cached entry has no canonical JSON, i.e.
  ///                 a forward-compat legacy entry) →
  ///                 [DispatchIdempotencyHit] with no new event emitted;
  ///               - mismatch → emit `idempotency_mismatch` denial
  ///                 event and return [DispatchIdempotencyMismatch].
  ///   Stage 5 — call `action.validate(parsedInput)`; on throw, emit
  ///             `validation_denied` and return [DispatchValidationDenied].
  ///   Stage 6 — for each `action.permissions`, await
  ///             `authorization.isPermitted`. First [Deny] short-circuits:
  ///             emit `authorization_denied` (with `permission_denied`,
  ///             optional `principal_active_role`, and `deny_reason`) and
  ///             return [DispatchAuthorizationDenied]. All-Allow falls
  ///             through to Stage 7.
  ///   Stage 7 — call `action.execute(parsedInput, ctx)`. On throw, emit
  ///             `execution_failed` and return [DispatchExecutionFailed].
  ///   Stage 8 — atomically persist all events from `result.events` in a
  ///             single `backend.transaction`. Each event is stamped with
  ///             `initiator`, `action_invocation_id`, `action_name`, and
  ///             `flowToken` (draft's, falling back to dispatch parameter).
  ///             On any append throw, the transaction rolls back and the
  ///             dispatcher emits `execution_failed`, returning
  ///             [DispatchExecutionFailed].
  ///   Stage 9  — if `action.idempotency != none` AND a key was supplied,
  ///             record an `IdempotencyEntry` via `idempotency.record`.
  ///             Entry carries `actionName`, `principalId`, `key`,
  ///             JSON-serialized result, `emittedEventIds`, and
  ///             `expiresAt = ctx.requestStartedAt + action.idempotencyTtl`.
  ///   Stage 10 — return `DispatchResult.success(result, emittedEventIds)`.
  Future<DispatchResult<Object?>> dispatch(
    ActionSubmission submission,
    ActionContext ctx,
  ) async {
    final actionName = submission.actionName;
    final rawInput = submission.rawInput;
    final idempotencyKey = submission.idempotencyKey;
    final flowToken = submission.flowToken;
    // Stage 2: invocation_id (generated up front so denials can carry it).
    final invocationId = _uuid.v4();
    final invocationMetadata = <String, Object?>{
      'action_invocation_id': invocationId,
      'action_name': actionName,
    };

    // Stage 1: lookup
    final action = registry.lookup(actionName);
    if (action == null) {
      final denial = denialUnknownAction(
        invocationId: invocationId,
        requestedName: actionName,
        actionInvocationMetadata: invocationMetadata,
      );
      await _persistDenial(denial, ctx, flowToken: flowToken);
      return DispatchResult<Object?>.unknownAction(actionName);
    }

    // Idempotency-required precondition check.
    // Must run BEFORE parseInput per spec: missing-required-key is a
    // parse-stage denial that fires before the action gets to see the raw
    // input.
    if (action.idempotency == Idempotency.required && idempotencyKey == null) {
      final error = MissingIdempotencyKeyError(actionName);
      final denial = denialParseDenied(
        invocationId: invocationId,
        actionName: actionName,
        error: error,
        actionInvocationMetadata: invocationMetadata,
      );
      await _persistDenial(denial, ctx, flowToken: flowToken);
      return DispatchResult<Object?>.parseDenied(error);
    }

    // Stage 3: parse
    final Object? parsedInput;
    try {
      parsedInput = action.parseInput(rawInput);
    } catch (error) {
      final denial = denialParseDenied(
        invocationId: invocationId,
        actionName: actionName,
        error: error,
        actionInvocationMetadata: invocationMetadata,
      );
      await _persistDenial(denial, ctx, flowToken: flowToken);
      return DispatchResult<Object?>.parseDenied(error);
    }

    // Stage 4: idempotency cache lookup
    // Skip entirely for Idempotency.none; also skip if no key was supplied
    // (covers Idempotency.optional with no key).
    //
    // Compute the submitted rawInput's canonical-JSON once here. It is
    // used both for the cache-hit content comparison (this stage) and,
    // on the success path, for recording alongside the cached outcome
    // (Stage 9) so a subsequent retry can be content-compared.
    String? submittedCanonicalJson;
    if (action.idempotency != Idempotency.none && idempotencyKey != null) {
      submittedCanonicalJson = canonicalize(rawInput);
      final entry = await idempotency.lookup(
        action.name,
        ctx.principal.id,
        idempotencyKey,
      );
      if (entry != null) {
        // Cache hit. Compare submitted canonical JSON against the
        // cached value to detect EVS-PRD-action-dispatch/E
        // mismatched-content collisions. A cached entry without a
        // recorded canonical JSON (rawInputCanonicalJson == null) is a
        // forward-compat legacy row — fall back to the plain cache-hit
        // behaviour so we don't synthesise a false denial.
        final cachedCanonicalJson = entry.rawInputCanonicalJson;
        if (cachedCanonicalJson == null ||
            cachedCanonicalJson == submittedCanonicalJson) {
          // Match (or legacy entry) — short-circuit; no new events emitted.
          return DispatchResult<Object?>.idempotencyHit(
            entry.resultJson,
            entry.emittedEventIds,
          );
        }
        // Mismatch: emit denial and return DispatchIdempotencyMismatch.
        final cachedHash = _sha256Hex(cachedCanonicalJson);
        final submittedHash = _sha256Hex(submittedCanonicalJson);
        final denial = denialIdempotencyMismatch(
          invocationId: invocationId,
          actionName: action.name,
          idempotencyKey: idempotencyKey,
          cachedRawInputHash: cachedHash,
          submittedRawInputHash: submittedHash,
          actionInvocationMetadata: invocationMetadata,
        );
        await _persistDenial(denial, ctx, flowToken: flowToken);
        return DispatchResult<Object?>.idempotencyMismatch(
          actionName: action.name,
          idempotencyKey: idempotencyKey,
          cachedRawInputHash: cachedHash,
          submittedRawInputHash: submittedHash,
        );
      }
      // Cache miss — fall through to Stage 5+.
    }

    // Stage 5: validate
    try {
      action.validate(parsedInput);
    } on Object catch (err) {
      final denial = denialValidationDenied(
        invocationId: invocationId,
        actionName: action.name,
        error: err,
        actionInvocationMetadata: invocationMetadata,
      );
      await _persistDenial(denial, ctx, flowToken: flowToken);
      return DispatchResult<Object?>.validationDenied(err);
    }

    // Stage 6: authorize (pre-tx scope-resolution check).
    //
    // Resolving the per-dispatch scope and validating it against the
    // permission's declared scopeClass is a pure, in-process check —
    // no projection reads. We do it BEFORE opening the dispatch tx so
    // the failure path (programmer bug in Action.scopeFor) doesn't
    // pay the cost of opening a tx for a single denial append.
    final principal = ctx.principal;
    final principalActiveRole = principal is UserPrincipal
        ? principal.activeRole
        : null;

    // Resolved per-permission scopes, in iteration order, to hand to
    // the policy inside the tx. List<ScopeValue?> because unscoped
    // permissions contribute a null entry. `action.permissions` is a
    // Set<Permission>, but {…}-literal Sets in Dart preserve insertion
    // order; we materialize to a List so the in-tx loop indexes in O(1)
    // and so the order matches the resolved-scopes order.
    final permissionList = action.permissions.toList(growable: false);
    final resolvedScopes = <ScopeValue?>[];
    for (final permission in permissionList) {
      ScopeValue? scopeValue;
      if (permission.scopeClass != null) {
        scopeValue = action.scopeFor(permission, parsedInput);
        final scopeDenyReason = _checkScopeResolution(
          permission: permission,
          resolved: scopeValue,
        );
        if (scopeDenyReason != null) {
          // Class mismatch / TotalWildcard: stamp the offending scope so
          // the audit record carries it. Null resolution: nothing to stamp.
          final denial = denialAuthorizationDenied(
            invocationId: invocationId,
            actionName: action.name,
            permission: permission,
            principalActiveRole: principalActiveRole,
            denyReason: scopeDenyReason,
            scopeValue: scopeValue,
            actionInvocationMetadata: invocationMetadata,
          );
          await _persistDenial(denial, ctx, flowToken: flowToken);
          return DispatchResult<Object?>.authorizationDenied(permission);
        }
      }
      resolvedScopes.add(scopeValue);
    }

    // Stage 6 (policy reads) + Stage 7 (execute) + Stage 8 (persist) run
    // in a single dispatch transaction. The policy receives the active
    // [Transaction] so its projection reads share the read-snapshot with the
    // event appends — a role/scope revocation committed by another
    // dispatch between authorize-reads and append takes effect on
    // subsequent dispatches, not the in-flight one.
    final emittedEventIds = <String>[];
    final initiator = ctx.principal.toInitiator();
    DispatchResult<Object?>? authorizationDenial;
    ExecutionResult<Object?>? executionResultHolder;
    Object? executeError;

    try {
      await events.runTransaction<void>((txn, collector) async {
        // Stage 6: policy-level authorize, inside the dispatch tx.
        for (var i = 0; i < permissionList.length; i++) {
          final permission = permissionList[i];
          final scopeValue = resolvedScopes[i];
          final decision = await authorization.isPermitted(
            principal,
            permission,
            scopeValue,
            txn: txn,
          );
          if (decision is Deny) {
            final denial = denialAuthorizationDenied(
              invocationId: invocationId,
              actionName: action.name,
              permission: decision.permission,
              principalActiveRole: principalActiveRole,
              denyReason: decision.reason,
              scopeValue: scopeValue,
              actionInvocationMetadata: invocationMetadata,
            );
            // Append the denial event in the same tx so the policy reads
            // and their recorded outcome commit atomically.
            await events.appendInTxn(
              txn,
              collector: collector,
              entryType: denial.entryType,
              aggregateId: denial.aggregateId,
              aggregateType: denial.aggregateType,
              eventType: denial.eventType,
              data: denial.data,
              initiator: ctx.principal.toInitiator(),
              flowToken: denial.flowToken ?? flowToken,
              metadata: denial.metadata,
              security: ctx.security,
              checkpointReason: null,
              changeReason: null,
              dedupeByContent: false,
            );
            authorizationDenial = DispatchResult<Object?>.authorizationDenied(
              decision.permission,
            );
            return; // commit the tx with just the denial event recorded.
          }
        }

        // Stage 7: execute, inside the dispatch tx. A throw here
        // bubbles out of runTransaction and rolls the tx back; we
        // capture the error and emit the execution_failed denial in
        // a separate append after the rollback (see catch block).
        //
        // We use a captured local for the error rather than letting the
        // raw exception propagate so the post-tx catch can short-circuit
        // its denial-emission path without conflating with backend
        // transaction-internal failures.
        late ExecutionResult<Object?> executionResult;
        try {
          executionResult = await action.execute(parsedInput, ctx);
        } on Object catch (err) {
          executeError = err;
          // Rethrow to abort the tx; the post-tx handler will emit the
          // execution_failed denial event.
          rethrow;
        }
        executionResultHolder = executionResult;

        // Stage 8: append the execute-result events in the same tx.
        final security =
            executionResult.securityDetailsOverride ?? ctx.security;
        for (final draft in executionResult.events) {
          final mergedMetadata = <String, Object?>{
            ...?draft.metadata,
            'action_invocation_id': invocationId,
            'action_name': action.name,
          };
          final stored = await events.appendInTxn(
            txn,
            collector: collector,
            entryType: draft.entryType,
            aggregateId: draft.aggregateId,
            aggregateType: draft.aggregateType,
            eventType: draft.eventType,
            data: draft.data,
            initiator: initiator,
            flowToken: draft.flowToken ?? flowToken,
            metadata: mergedMetadata,
            security: security,
            checkpointReason: null,
            changeReason: null,
            dedupeByContent: false,
          );
          if (stored != null) {
            emittedEventIds.add(stored.eventId);
          }
        }
      });
    } on Object catch (err) {
      // Transaction was rolled back by the backend. Distinguish:
      //   - execute() threw  → emit execution_failed denial (we captured
      //                        the original error in executeError).
      //   - append/other     → still surface as execution_failed; the
      //                        original error is what the backend
      //                        rethrew.
      final reportedError = executeError ?? err;
      final denial = denialExecutionFailed(
        invocationId: invocationId,
        actionName: action.name,
        error: reportedError,
        actionInvocationMetadata: invocationMetadata,
      );
      await _persistDenial(denial, ctx, flowToken: flowToken);
      return DispatchResult<Object?>.executionFailed(reportedError);
    }

    // Authorize-stage denial committed; return the captured outcome.
    if (authorizationDenial != null) {
      return authorizationDenial!;
    }
    final executionResult = executionResultHolder!;

    // Stage 9: record idempotency
    if (action.idempotency != Idempotency.none && idempotencyKey != null) {
      final resultJson = _resultToJson(executionResult.result);
      await idempotency.record(
        actionName: action.name,
        principalId: ctx.principal.id,
        key: idempotencyKey,
        resultJson: resultJson,
        emittedEventIds: emittedEventIds,
        expiresAt: ctx.requestStartedAt.add(action.idempotencyTtl),
        // submittedCanonicalJson was computed in Stage 4 above; persist
        // it so subsequent retries can be content-compared against the
        // original submission (EVS-PRD-action-dispatch/E).
        rawInputCanonicalJson: submittedCanonicalJson,
      );
    }

    // Stage 10: return success
    return DispatchResult<Object?>.success(
      executionResult.result,
      emittedEventIds,
    );
  }

  /// SHA-256 hex digest of [canonicalJson] (a string already produced by
  /// the substrate's RFC-8785 canonicalizer). Used to stamp opaque
  /// fingerprints onto `idempotency_mismatch` denial events so the audit
  /// log records WHICH content collided without persisting the
  /// (potentially sensitive) inputs themselves.
  static String _sha256Hex(String canonicalJson) =>
      sha256.convert(utf8.encode(canonicalJson)).toString();

  /// Converts an action result to a JSON-serializable map for idempotency
  /// recording. Handles null, `Map<String, Object?>`, objects with `.toJson()`,
  /// and falls back to `{'value': result.toString()}`.
  Map<String, Object?> _resultToJson(Object? result) {
    if (result == null) return const <String, Object?>{};
    if (result is Map<String, Object?>) return result;
    // Try duck-typing .toJson() before falling back to toString.
    // Catching Object and checking type avoids the lint against catching Error
    // subclasses directly while still handling the missing-method case.
    try {
      // ignore: avoid_dynamic_calls
      final json = (result as dynamic).toJson() as Map<String, Object?>;
      return json;
    } on Object catch (e) {
      if (e is NoSuchMethodError) {
        return <String, Object?>{'value': result.toString()};
      }
      rethrow;
    }
  }

  /// Validates the [resolved] scope returned by `Action.scopeFor` against
  /// the [permission]'s declared `scopeClass`. Returns null if the scope is
  /// usable; returns `DenyReason.scopeUnresolvable` for any resolution
  /// failure the dispatcher must short-circuit on.
  ///
  /// Caller has already established that `permission.scopeClass != null`.
  /// Failure shapes:
  ///   - null                                     → action didn't supply a
  ///                                                scope value for a
  ///                                                scoped permission.
  ///   - TotalWildcardScope                       → carries no class_ to
  ///                                                match against
  ///                                                permission.scopeClass.
  ///   - BoundScope / ValueWildcardScope with
  ///     class_ != permission.scopeClass          → scope class mismatch.
  DenyReason? _checkScopeResolution({
    required Permission permission,
    required ScopeValue? resolved,
  }) {
    if (resolved == null) {
      return DenyReason.scopeUnresolvable;
    }
    switch (resolved) {
      case TotalWildcardScope():
        return DenyReason.scopeUnresolvable;
      case BoundScope(class_: final cls):
        if (cls != permission.scopeClass) {
          return DenyReason.scopeUnresolvable;
        }
        return null;
      case ValueWildcardScope(class_: final cls):
        if (cls != permission.scopeClass) {
          return DenyReason.scopeUnresolvable;
        }
        return null;
    }
  }

  /// Persists a denial event through [events]. Single event, atomic by
  /// virtue of EventStore.append's own transaction. The substrate stamps
  /// `entry_type_version` from the registry's `registeredVersion` for
  /// `draft.entryType`; the caller does not supply it.
  Future<void> _persistDenial(
    EventDraft draft,
    ActionContext ctx, {
    String? flowToken,
  }) async {
    await events.append(
      entryType: draft.entryType,
      aggregateId: draft.aggregateId,
      aggregateType: draft.aggregateType,
      eventType: draft.eventType,
      data: draft.data,
      initiator: ctx.principal.toInitiator(),
      flowToken: draft.flowToken ?? flowToken,
      metadata: draft.metadata,
      security: ctx.security,
    );
  }
}
