import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Server-side channel whose sink behaves like a real WebSocket sink:
/// once the peer has gone away or the sink was closed, `add` throws
/// `StateError('Cannot add event after closing')`.
class _ClosingChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  _ClosingChannel() {
    sink = _ClosingSink(this);
  }

  // Closed by the test body; the lint cannot see the call site.
  // ignore: close_sinks
  final StreamController<Object?> incoming = StreamController<Object?>();
  final List<Object?> sent = <Object?>[];
  bool closed = false;

  @override
  Stream<dynamic> get stream => incoming.stream;

  @override
  // ignore: close_sinks
  late final WebSocketSink sink;

  @override
  int? get closeCode => null;

  @override
  String? get closeReason => null;

  @override
  String? get protocol => null;

  @override
  Future<void> get ready => Future<void>.value();
}

class _ClosingSink implements WebSocketSink {
  _ClosingSink(this._channel);

  final _ClosingChannel _channel;

  @override
  void add(Object? event) {
    if (_channel.closed) {
      throw StateError('Cannot add event after closing');
    }
    _channel.sent.add(event);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<dynamic> addStream(Stream<Object?> stream) async {}

  @override
  Future<dynamic> close([int? closeCode, String? closeReason]) async {
    _channel.closed = true;
  }

  @override
  Future<dynamic> get done => Future<void>.value();
}

/// Validator whose `authenticate` completes only when [release] is called.
class _GatedValidator implements PrincipalAuthValidator {
  final Completer<Principal> _gate = Completer<Principal>();

  void release() => _gate.complete(
    UserPrincipal(
      userId: 'alice',
      roles: const {'install'},
      activeRole: 'install',
    ),
  );

  @override
  Future<Principal> authenticate(String credential) => _gate.future;
}

class _NoStore implements EventStore {
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _AllowPolicy implements AuthorizationPolicy {
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  // A send queued behind authentication is dropped when the connection
  // closed before it ran; no error escapes and nothing is written.
  test(
    'client closing while authentication is pending drops the send',
    () async {
      final errors = <Object>[];
      final registry = WsConnectionRegistry();
      await runZonedGuarded(() async {
        final channel = _ClosingChannel();
        final validator = _GatedValidator();
        runSubscriptionHandler(
          channel: channel,
          validator: validator,
          eventStore: _NoStore(),
          policy: _AllowPolicy(),
          viewScopes: ViewScopeRegistry(),
          viewPermissionNamer: (v) => null,
          connectionRegistry: registry,
        );

        channel.incoming.add(jsonEncode({'type': 'auth', 'credential': 'x'}));
        await pumpEventQueue();

        // The client disconnects while authenticate is still pending.
        channel.closed = true;
        await channel.incoming.close();
        await pumpEventQueue();

        validator.release();
        await pumpEventQueue();

        expect(channel.sent, isEmpty);
      }, (Object e, StackTrace _) => errors.add(e));

      expect(errors, isEmpty);
    },
  );

  // The sink closes without the stream's onDone being delivered; the send
  // queued behind authentication hits the closed sink and is dropped.
  test('a send hitting a closed sink is dropped without an error', () async {
    final errors = <Object>[];
    late _ClosingChannel channel;
    await runZonedGuarded(() async {
      channel = _ClosingChannel();
      final validator = _GatedValidator();
      runSubscriptionHandler(
        channel: channel,
        validator: validator,
        eventStore: _NoStore(),
        policy: _AllowPolicy(),
        viewScopes: ViewScopeRegistry(),
        viewPermissionNamer: (v) => null,
        connectionRegistry: WsConnectionRegistry(),
      );

      channel.incoming.add(jsonEncode({'type': 'auth', 'credential': 'x'}));
      await pumpEventQueue();

      channel.closed = true;
      validator.release();
      await pumpEventQueue();
    }, (Object e, StackTrace _) => errors.add(e));

    expect(errors, isEmpty);
    expect(channel.sent, isEmpty);
  });
}
