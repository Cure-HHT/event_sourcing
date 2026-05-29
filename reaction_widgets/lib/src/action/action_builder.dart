// Implements: EVS-PRD-reaction-widget-contract/C, /E, /G

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

/// Builder function for [ActionBuilder]. Receives the current
/// [ActionState] and a `submit` callback that triggers a submission.
typedef ActionBuilderFn =
    Widget Function(
      BuildContext context,
      ActionState state,
      void Function() submit,
    );

/// Headless Builder primitive for action submission.
///
/// Owns:
/// - The [ActionState] lifecycle ([Idle] -> [Submitting] ->
///   [Success] | [Denied] | [Failed]).
/// - The idempotency-key lifetime: generates one UUID v4 at first
///   submit, reuses it for retries during [Submitting], regenerates
///   after a terminal state. Consumers may override by setting
///   `idempotencyKey` on the [ActionSubmission] returned by
///   [submissionFactory].
/// - Dispose-time cancellation: in-flight submission results that
///   arrive after dispose are silently discarded.
///
/// Renders nothing of its own (headless, per
/// EVS-PRD-reaction-widget-contract/G). Delegates rendering entirely
/// to [builder].
class ActionBuilder extends StatefulWidget {
  const ActionBuilder({
    super.key,
    required this.submissionFactory,
    required this.builder,
    this.idempotencyKeyGenerator,
  });

  /// Factory that produces the [ActionSubmission] to dispatch on each
  /// submit. Called per submit() invocation. If the returned submission
  /// has a non-null `idempotencyKey`, that key is used as-is and the
  /// generator caching policy is bypassed (consumer-supplied keys
  /// override per EVS-PRD-reaction-widget-contract/E).
  final ActionSubmission Function() submissionFactory;

  /// Caller-supplied renderer. Receives the current [ActionState] and
  /// a `submit` callback.
  final ActionBuilderFn builder;

  /// Optional override for the idempotency-key generator. Defaults to
  /// [UuidIdempotencyKeyGenerator]. Tests typically inject a
  /// deterministic stub.
  final IdempotencyKeyGenerator? idempotencyKeyGenerator;

  @override
  State<ActionBuilder> createState() => _ActionBuilderState();
}

class _ActionBuilderState extends State<ActionBuilder> {
  ActionState _state = const Idle();

  /// Cached generated key for the in-flight submission. Set on the
  /// first submit() of a Submitting phase, cleared on terminal state.
  /// Not used when the submissionFactory supplies its own
  /// `idempotencyKey`.
  String? _activeKey;

  late IdempotencyKeyGenerator _keyGen;

  @override
  void initState() {
    super.initState();
    _keyGen = widget.idempotencyKeyGenerator ?? UuidIdempotencyKeyGenerator();
  }

  void _submit() {
    final scope = ReActionScope.of(context);
    final base = widget.submissionFactory();
    final consumerKey = base.idempotencyKey;
    final ActionSubmission sub;
    if (consumerKey != null) {
      // Consumer-supplied key: use as-is, bypass the generator/cache.
      sub = base;
    } else {
      // Generated-key path: mint on first submit of a Submitting phase,
      // reuse for retries until terminal state clears _activeKey.
      final key = _activeKey ??= _keyGen.generate();
      sub = ActionSubmission(
        actionName: base.actionName,
        rawInput: base.rawInput,
        idempotencyKey: key,
        flowToken: base.flowToken,
      );
    }

    setState(() => _state = const Submitting());
    scope.actionSubmitter
        .submit(sub)
        .then((result) {
          if (!mounted) return;
          setState(() {
            _state = switch (result) {
              DispatchSuccess() => Success(result),
              DispatchIdempotencyHit() => Success(result),
              _ => Denied(result),
            };
            if (consumerKey == null) _activeKey = null;
          });
        })
        .catchError((Object e, StackTrace st) {
          if (!mounted) return;
          setState(() {
            _state = Failed(e, st);
            if (consumerKey == null) _activeKey = null;
          });
        });
  }

  @override
  Widget build(BuildContext context) =>
      widget.builder(context, _state, _submit);
}
