// Implements: EVS-PRD-reaction-widget-contract/G (error-sink sub-clause)

import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

/// Callback fired by [ReActionErrorListener] in response to an auth or
/// transport transition. Receives the listener's [BuildContext]; the
/// callback is invoked from a post-frame callback, so it is safe to use
/// the context for navigation (e.g., `Navigator.of(context).push(...)`).
typedef ReActionErrorCallback = void Function(BuildContext context);

/// Centralised sink for auth and transport error transitions.
///
/// Subscribes to the composed [ReactionScope]'s [AuthSession.stream] and
/// [ReactionScope.connectionStatusStream], and fires the corresponding
/// caller-supplied callback on each transition. Callbacks are dispatched
/// via `WidgetsBinding.instance.addPostFrameCallback`, which guarantees
/// they run AFTER the current build/layout/paint phase has completed —
/// so they may safely navigate or call into other [BuildContext]-using
/// APIs that are illegal during build (e.g.,
/// `Navigator.of(context).push`).
///
/// Headless per `EVS-PRD-reaction-widget-contract`-G: this widget renders
/// ONLY [child]; it adds no decoration of its own and does NOT rebuild
/// [child] in response to auth or transport transitions.
class ReActionErrorListener extends StatefulWidget {
  const ReActionErrorListener({
    super.key,
    required this.child,
    this.onAuthExpired,
    this.onAuthNotAuthenticated,
    this.onTransportReconnecting,
    this.onTransportDisconnected,
  });

  /// The subtree to render. [ReActionErrorListener] does not rebuild
  /// [child] in response to auth or transport transitions — that is
  /// the entire point of the imperative shape; downstream consumers
  /// wrap with their own modal/snackbar/navigation sugar.
  final Widget child;

  /// Fired when `AuthSession.stream` transitions to [Expired]. App
  /// should typically route to re-auth without discarding app-side
  /// state.
  final ReActionErrorCallback? onAuthExpired;

  /// Fired when `AuthSession.stream` transitions to [NotAuthenticated].
  /// App should typically route to login.
  final ReActionErrorCallback? onAuthNotAuthenticated;

  /// Fired when `connectionStatusStream` transitions to [Reconnecting].
  /// App may surface a passive "reconnecting…" indicator.
  final ReActionErrorCallback? onTransportReconnecting;

  /// Fired when `connectionStatusStream` transitions to [Disconnected]
  /// (the reconnect policy has given up). App should surface an
  /// actionable error.
  final ReActionErrorCallback? onTransportDisconnected;

  @override
  State<ReActionErrorListener> createState() => _ReActionErrorListenerState();
}

class _ReActionErrorListenerState extends State<ReActionErrorListener> {
  StreamSubscription<AuthStatus>? _authSub;
  StreamSubscription<ConnectionStatus>? _statusSub;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_authSub != null) return;
    final scope = ReActionScope.of(context);
    _authSub = scope.authSession.stream.listen(_onAuth);
    _statusSub = scope.connectionStatusStream.listen(_onStatus);
  }

  void _postFrame(ReActionErrorCallback? cb) {
    if (cb == null) return;
    final binding = WidgetsBinding.instance;
    binding.addPostFrameCallback((_) {
      if (!mounted) return;
      cb(context);
    });
    // The widget tree may be idle (no setState pending), in which case
    // pumping doesn't schedule a new frame and the post-frame callback
    // never fires. Explicitly requesting a frame guarantees the
    // callback executes regardless of tree state. Cheap no-op when a
    // frame is already pending.
    binding.scheduleFrame();
  }

  void _onAuth(AuthStatus s) {
    switch (s) {
      case Expired():
        _postFrame(widget.onAuthExpired);
      case NotAuthenticated():
        _postFrame(widget.onAuthNotAuthenticated);
      case Authenticated():
        // No-op: not an error transition.
        break;
    }
  }

  void _onStatus(ConnectionStatus s) {
    switch (s) {
      case Reconnecting():
        _postFrame(widget.onTransportReconnecting);
      case Disconnected():
        _postFrame(widget.onTransportDisconnected);
      case Connected():
        // No-op: not an error transition.
        break;
    }
  }

  @override
  void dispose() {
    _authSub?.cancel();
    _statusSub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.child;
}
