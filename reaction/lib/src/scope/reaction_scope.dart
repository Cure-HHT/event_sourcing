// Implements: EVS-PRD-reaction-scope/A

import 'dart:async';

import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/scope/connection_status.dart';

/// Substrate-agnostic composition root that bundles the four library
/// interfaces with live transport-connection state.
///
/// Two shipped impls:
///
/// - [LocalScope]  — in-process composition (Local* impls); always
///                   reports [Connected].
/// - [RemoteScope] — cross-process composition (Remote* impls over a
///                   shared WS); drives [ConnectionStatus] from WS
///                   lifecycle events.
///
/// Consumer code (especially the `reaction_widgets` layer) depends on
/// this interface, not on the concrete scope types — that is what makes
/// widget code source-identical across Local and Remote per the
/// substrate-agnostic widget contract (`EVS-PRD-reaction-widget-contract`-B).
abstract interface class ReactionScope {
  AuthSession get authSession;
  ActionSubmitter get actionSubmitter;
  ViewSource get viewSource;
  PermissionSource get permissionSource;

  /// Current connection state, synchronous (always non-null).
  ConnectionStatus get connectionStatus;

  /// Stream of subsequent [ConnectionStatus] transitions.
  ///
  /// Does NOT emit the current value on subscribe; consumers that need
  /// "current plus subsequent" should seed from [connectionStatus] and
  /// listen to this stream. Implementations SHOULD be broadcast streams
  /// (single producer, multiple widget consumers).
  Stream<ConnectionStatus> get connectionStatusStream;

  /// Graceful teardown. After [dispose], the four interface getters and
  /// `connectionStatus*` SHOULD throw [StateError] on access.
  Future<void> dispose();
}
