// Implements: EVS-PRD-action-dispatch/A (action interface that dispatch accepts)
// Implements: EVS-PRD-action-dispatch/B (defines the parse/validate/execute stages)

import 'package:event_sourcing/src/actions/action_context.dart';
import 'package:event_sourcing/src/actions/execution_result.dart';
import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';

/// A portal command. Concrete subclasses define one action and one
/// command shape (`TInput`) and one result shape (`TResult`).
///
/// Lifecycle inside `ActionDispatcher`:
///   parseInput(raw)        -> TInput            (pure)
///   validate(input)        throws on invalid    (pure)
///   authorize via policy                        (uses [permissions])
///   execute(input, ctx)    -> ExecutionResult   (effectful; returns
///                                                events for atomic
///                                                persistence)
//
//             permissions/idempotency + parseInput/validate/execute methods.
//             (no I/O; enforced by review, not the type system).
abstract class Action<TInput, TResult> {
  const Action();

  /// Stable identifier; appears in event metadata as `action_name` and
  /// is the lookup key in `ActionRegistry`.
  String get name;

  /// Human-readable description for admin UIs and discovery output.
  String get description;

  /// Permissions required by this action. The dispatcher's authorize
  /// stage checks every permission in this set against the
  /// `AuthorizationPolicy`; the first denial short-circuits.
  Set<Permission> get permissions;

  /// How the dispatcher treats `idempotencyKey` for calls to this
  /// action: ignore (`none`), use if supplied (`optional`), or demand
  /// (`required`).
  Idempotency get idempotency;

  /// Per-action TTL override for idempotency cache entries.
  /// Default: 24 hours.
  Duration get idempotencyTtl => defaultIdempotencyTtl;

  /// Parse raw input into the typed shape. Pure: no I/O, no global
  /// state. Throws (typically `FormatException` or `ArgumentError`)
  /// on malformed input; the dispatcher converts the throw into a
  /// `parse_denied` denial event.
  TInput parseInput(Map<String, Object?> raw);

  /// Validate the typed input. Pure: no I/O. Throws on invalid input
  /// (typically `ValidationError`); the dispatcher converts the throw
  /// into a `validation_denied` denial event.
  void validate(TInput input);

  /// Run the action. Returns `ExecutionResult` carrying the typed
  /// result and the events to persist atomically (one transaction in
  /// the events lib).
  Future<ExecutionResult<TResult>> execute(TInput input, ActionContext ctx);

  /// Per dispatch, supply the scope value for each scoped permission this
  /// action requires. Pure: no I/O. Returns null for unscoped permissions
  /// (default impl). For scoped permissions, the returned `ScopeValue`'s
  /// `scopeClass` MUST equal the permission's declared `scopeClass`;
  /// mismatch is `Deny(scopeUnresolvable)`.
  ///
  /// `TotalWildcardScope` MUST NOT be returned (carries no class_ to
  /// match against permission.scopeClass); dispatcher denies as
  /// scopeUnresolvable if returned. `ValueWildcardScope` is technically
  /// valid when an action genuinely operates on all values of a class.
  ScopeValue? scopeFor(Permission perm, TInput input) => null;
}
