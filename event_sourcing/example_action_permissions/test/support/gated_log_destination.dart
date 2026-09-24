// A demo destination whose sends a test holds and releases. This file
// declares no tests, so it carries no citation.

import 'dart:async';

import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:event_sourcing/event_sourcing.dart';

/// A [LogDestination] that, while [hold] is set, keeps each send in flight
/// until [release] is called.
class GatedLogDestination extends LogDestination {
  GatedLogDestination() : super(sink: _noLog);

  static void _noLog(String _) {}

  bool hold = false;
  Completer<void> _gate = Completer<void>();
  Completer<void> _started = Completer<void>();

  /// The lines of the batches delivered so far.
  final List<String> delivered = <String>[];

  /// Completes when a held send has started.
  Future<void> get sendStarted => _started.future;

  /// Lets every held send finish, and stops holding.
  void release() {
    hold = false;
    if (!_gate.isCompleted) _gate.complete();
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    if (hold) {
      if (!_started.isCompleted) _started.complete();
      await _gate.future;
      _gate = Completer<void>();
      _started = Completer<void>();
    }
    final result = await super.send(payload);
    if (result is SendOk) delivered.add(String.fromCharCodes(payload.bytes));
    return result;
  }
}
