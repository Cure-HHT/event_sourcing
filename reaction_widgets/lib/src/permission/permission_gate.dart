// Implements: EVS-PRD-reaction-widget-contract/G
// the permission-gate sub-clause.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

/// Gates [child] on whether the active Principal holds a permission named
/// [permission].
///
/// Reactive: re-renders when [PermissionSource.stream] emits a new
/// [EffectiveAuthorization]. Returns [fallback] (default
/// `SizedBox.shrink()`) when the permission is not held or when the
/// active permission snapshot is `null` (i.e., not authenticated / not
/// loaded yet).
///
/// The check is **name-only**: it tests whether `auth.rolePermissions`
/// contains any [Permission] with the matching `name`, ignoring
/// `scopeClass`. UI gating reads coarse "can the user do X at all?"
/// — fine-grained per-scope filtering still belongs in the calling
/// app, and per-decision authorization still flows through the
/// substrate at dispatch time (see [EffectiveAuthorization]).
///
/// Headless per `EVS-PRD-reaction-widget-contract`-G: this widget renders
/// ONLY [child] or [fallback]; it adds no decoration of its own.
class PermissionGate extends StatefulWidget {
  const PermissionGate({
    super.key,
    required this.permission,
    required this.child,
    this.fallback = const SizedBox.shrink(),
  });

  /// The permission name to gate on (e.g. `patient.edit`). Matched
  /// case-sensitively against the `name` field of every [Permission] in
  /// the active `EffectiveAuthorization.rolePermissions` set.
  final String permission;

  /// Rendered when the active Principal holds [permission].
  final Widget child;

  /// Rendered when the active Principal does NOT hold [permission], or
  /// when no `EffectiveAuthorization` snapshot is available yet
  /// (`PermissionSource.current == null`). Defaults to
  /// `SizedBox.shrink()`.
  final Widget fallback;

  @override
  State<PermissionGate> createState() => _PermissionGateState();
}

class _PermissionGateState extends State<PermissionGate> {
  StreamSubscription<EffectiveAuthorization?>? _sub;
  EffectiveAuthorization? _auth;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_sub != null) return;
    final scope = ReActionScope.of(context);
    _auth = scope.permissionSource.current;
    _sub = scope.permissionSource.stream.listen((auth) {
      if (!mounted) return;
      setState(() => _auth = auth);
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  bool _holdsPermission() {
    final auth = _auth;
    if (auth == null) return false;
    // Permission equality is name-based (see
    // event_sourcing/lib/src/actions/permission.dart), so `.contains` on
    // a synthetic name-only Permission gives the same answer as iterating
    // and comparing names by hand — but is less surprising to readers if
    // we walk the set explicitly.
    for (final p in auth.rolePermissions) {
      if (p.name == widget.permission) return true;
    }
    return false;
  }

  @override
  Widget build(BuildContext context) =>
      _holdsPermission() ? widget.child : widget.fallback;
}
