// Implements: EVS-PRD-action-submitter/C
// RemoteActionSubmitter submits
//   via HTTP POST /actions and decodes the DispatchResult from the
//   response body via DispatchResultCodec.
// Implements: EVS-PRD-action-submitter/D
// every outbound submission
//   includes the bearer credential from the co-mounted AuthSession
//   (RemoteConnection.httpPost injects it on every call).
// Implements: EVS-PRD-cross-process-event-transport/F
// wire-level bearer credential carriage on action submission.
// Implements: EVS-PRD-cross-process-event-transport/K
// a 503 view_converging response delivers a typed, transient
//   ViewConvergingRefusal naming the view to the submit() caller,
//   immediately, with no retry by default (`maxConvergingRetries: 0`).
//   The opt-in automatic retry (`maxConvergingRetries > 0`) re-sends
//   the same, unchanged ActionSubmission — so its idempotency key
//   never changes across attempts — with a capped-backoff wait,
//   surfacing each refusal on `convergingStream` while it waits, and
//   delivers the typed refusal to the caller once the attempt bound is
//   reached or `dispose()` cancels the wait.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';
import 'package:reaction/src/wire/view_converging_codec.dart';

class RemoteActionSubmitter implements ActionSubmitter {
  RemoteActionSubmitter({
    required this.connection,
    required this.authSession,
    this.maxConvergingRetries = 0,
    RetryScheduler? scheduleRetry,
  }) : _scheduleRetry = scheduleRetry ?? Timer.new;

  final RemoteConnection connection;
  final AuthSession authSession;

  /// The bound on automatic retries of a `view_converging` refusal.
  /// `0` (the default) submits once and delivers the typed refusal to
  /// the caller with no retry, per
  /// `EVS-PRD-cross-process-event-transport/K`. A positive value opts
  /// in to up to that many additional re-sends, bounded, with a
  /// backoff between them.
  final int maxConvergingRetries;

  final RetryScheduler _scheduleRetry;

  /// Backoff base and cap for the opt-in converging retry: doubles
  /// from [_retryBaseDelay] up to [_retryMaxDelay] per attempt.
  static const Duration _retryBaseDelay = Duration(milliseconds: 200);
  static const Duration _retryMaxDelay = Duration(seconds: 5);

  final StreamController<ViewConvergingRefusal> _convergingController =
      StreamController<ViewConvergingRefusal>.broadcast();

  /// Emits each `view_converging` refusal a submission meets while an
  /// opt-in automatic retry (`maxConvergingRetries > 0`) waits out its
  /// backoff before re-sending, so a caller can surface "still
  /// converging" state meanwhile.
  Stream<ViewConvergingRefusal> get convergingStream =>
      _convergingController.stream;

  bool _isDisposed = false;

  /// Every retry wait currently parked in [_wait], keyed by the
  /// [Completer] `submit()` awaits, to the [Timer] scheduled to
  /// complete it. [dispose] cancels each timer and completes each
  /// completer so no in-flight `submit()` call is left waiting forever,
  /// and so cancelling the timer is observable rather than merely
  /// relying on the `_isDisposed` check below to suppress a resend.
  final Map<Completer<void>, Timer> _pendingWaits = {};

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    if (authSession.current is! Authenticated) {
      throw const TransportException('not authenticated');
    }
    final url = connection.baseUrl.replace(path: '/actions');
    final body = jsonEncode(ActionSubmissionCodec.encode(submission));
    var attempt = 0;
    while (true) {
      final res = await connection.httpPost(url, body: body);
      if (res.statusCode == 401) {
        if (authSession is RemoteAuthSession) {
          (authSession as RemoteAuthSession).handleWireUnauthorized();
        }
        throw const TransportException('unauthorized');
      }
      if (res.statusCode == 503) {
        final refusal = decodeViewConvergingBody(res.body);
        if (refusal != null) {
          if (attempt >= maxConvergingRetries || _isDisposed) throw refusal;
          if (!_convergingController.isClosed) {
            _convergingController.add(refusal);
          }
          await _wait(_backoffDelay(attempt));
          if (_isDisposed) throw refusal;
          attempt++;
          continue;
        }
      }
      if (res.statusCode != 200) {
        throw TransportException('http ${res.statusCode}');
      }
      return DispatchResultCodec.decode(
        jsonDecode(res.body) as Map<String, Object?>,
      );
    }
  }

  /// Stops the opt-in converging retry and closes [convergingStream].
  /// Cancels every pending retry timer and unblocks every `submit()`
  /// call currently parked in [_wait]: each then observes [_isDisposed]
  /// and throws the `ViewConvergingRefusal` it last met instead of
  /// re-sending. Call once this submitter is no longer used (mirrors
  /// `RemotePermissionSource.dispose`'s stream cleanup).
  Future<void> dispose() async {
    _isDisposed = true;
    final pending = _pendingWaits.entries.toList();
    _pendingWaits.clear();
    for (final entry in pending) {
      entry.value.cancel();
      if (!entry.key.isCompleted) entry.key.complete();
    }
    await _convergingController.close();
  }

  Future<void> _wait(Duration delay) {
    final completer = Completer<void>();
    final timer = _scheduleRetry(delay, () {
      _pendingWaits.remove(completer);
      if (!completer.isCompleted) completer.complete();
    });
    _pendingWaits[completer] = timer;
    return completer.future;
  }

  static Duration _backoffDelay(int attempt) {
    final scaled = _retryBaseDelay * (1 << attempt);
    return scaled > _retryMaxDelay ? _retryMaxDelay : scaled;
  }
}
