// Verifies: WsConnectionRegistry tracks connections by userId and
// supports multi-connection-per-user fan-out for AuthorizationWatcher routing.
//
// Verifies: EVS-DEV-authz-watcher/A/B/C/E — the per-userId index the
//   AuthorizationWatcher routes close-frames and stale_data envelopes through.

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class _StubChannel implements WebSocketChannel {
  @override
  dynamic noSuchMethod(Invocation i) =>
      throw UnimplementedError(i.memberName.toString());
}

void main() {
  test('register + lookup returns the channel', () {
    final r = WsConnectionRegistry();
    final ch = _StubChannel();
    r.register('alice', ch);
    expect(r.channelsFor('alice').contains(ch), isTrue);
  });

  test('unregister removes the channel', () {
    final r = WsConnectionRegistry();
    final ch = _StubChannel();
    r
      ..register('alice', ch)
      ..unregister('alice', ch);
    expect(r.channelsFor('alice'), isEmpty);
  });

  test('multiple connections per user supported', () {
    final r = WsConnectionRegistry();
    final ch1 = _StubChannel();
    final ch2 = _StubChannel();
    r
      ..register('alice', ch1)
      ..register('alice', ch2);
    expect(r.channelsFor('alice').length, 2);
  });
}
