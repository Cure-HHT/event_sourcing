// Verifies: EVS-PRD-reaction-scope/B

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

void main() {
  group('ConnectionStatus', () {
    test('has three sealed variants', () {
      const ConnectionStatus a = Connected();
      const ConnectionStatus b = Reconnecting();
      const ConnectionStatus c = Disconnected();

      expect(a, isA<Connected>());
      expect(b, isA<Reconnecting>());
      expect(c, isA<Disconnected>());
    });

    test('variants are equal by type (const constructors)', () {
      expect(const Connected(), equals(const Connected()));
      expect(const Reconnecting(), equals(const Reconnecting()));
      expect(const Disconnected(), equals(const Disconnected()));
      expect(const Connected(), isNot(equals(const Reconnecting())));
    });

    test('exhaustive switch compiles for all three', () {
      String label(ConnectionStatus s) => switch (s) {
        Connected() => 'connected',
        Reconnecting() => 'reconnecting',
        Disconnected() => 'disconnected',
      };
      expect(label(const Connected()), 'connected');
      expect(label(const Reconnecting()), 'reconnecting');
      expect(label(const Disconnected()), 'disconnected');
    });
  });
}
