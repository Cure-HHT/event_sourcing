import 'package:provenance/provenance.dart';
import 'package:test/test.dart';

void main() {
  ProvenanceEntry makeEntry(String hop, {String identifier = 'id'}) =>
      ProvenanceEntry(
        hop: hop,
        receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
        identifier: identifier,
        softwareVersion: 'pkg@1.0.0',
      );

  group('appendHop', () {
    test('appending to an empty chain yields a one-entry list', () {
      final entry = makeEntry('mobile-device');
      final result = appendHop(const <ProvenanceEntry>[], entry);

      expect(result, [entry]);
      expect(result.length, 1);
    });

    test('appending to a non-empty chain adds exactly one entry', () {
      final first = makeEntry('mobile-device');
      final second = makeEntry('diary-server');
      final third = makeEntry('portal-server');

      final start = appendHop(const <ProvenanceEntry>[], first);
      final afterSecond = appendHop(start, second);
      final afterThird = appendHop(afterSecond, third);

      expect(start.length, 1);
      expect(afterSecond.length, 2);
      expect(afterThird.length, 3);
    });

    test('the new entry is placed at the tail', () {
      final first = makeEntry('mobile-device');
      final second = makeEntry('diary-server');

      final chain = appendHop(appendHop([], first), second);

      expect(chain.first, first);
      expect(chain.last, second);
    });

    test('appendHop does not mutate the input chain', () {
      final first = makeEntry('mobile-device');
      final second = makeEntry('diary-server');
      final start = appendHop([], first);
      final before = List<ProvenanceEntry>.of(start);

      appendHop(start, second);

      expect(start, equals(before));
      expect(start.length, 1);
    });

    // downstream callers from breaking the invariant.
    test('returned list rejects mutations (add, remove, replace)', () {
      final entry = makeEntry('mobile-device');
      final chain = appendHop([], entry);

      expect(
        () => chain.add(makeEntry('diary-server')),
        throwsUnsupportedError,
      );
      expect(chain.removeLast, throwsUnsupportedError);
      expect(() => chain[0] = makeEntry('replaced'), throwsUnsupportedError);
    });

    test('returns a new List instance, not the input', () {
      final start = <ProvenanceEntry>[];
      final result = appendHop(start, makeEntry('mobile-device'));

      expect(identical(start, result), isFalse);
    });

    // two separate positions in the chain.
    test('appending an equal entry twice yields two positions', () {
      final entry = makeEntry('mobile-device');
      final chain = appendHop(appendHop([], entry), entry);

      expect(chain.length, 2);
      expect(chain[0], entry);
      expect(chain[1], entry);
    });

    // equality) across multiple appends.
    test(
      'prior entries remain equal to their original value across appends',
      () {
        final first = makeEntry('mobile-device', identifier: 'device-abc');
        final originalFirst = first;

        var chain = appendHop([], first);
        chain = appendHop(chain, makeEntry('diary-server'));
        chain = appendHop(chain, makeEntry('portal-server'));

        expect(chain[0], equals(originalFirst));
      },
    );
  });
}
