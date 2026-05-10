import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

PromoterSpec _spec(int from, int to) => PromoterSpec(
  viewName: 'v',
  entryType: 't',
  fromVersion: from,
  toVersion: to,
  transforms: const [],
);

void main() {
  group('PromoterRegistry', () {
    test('chain returns specs in order from -> to', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      reg.register(_spec(2, 3));
      reg.register(_spec(3, 4));
      final chain = reg.chain(
        viewName: 'v',
        entryType: 't',
        fromVersion: 1,
        toVersion: 4,
      );
      expect(chain.map((s) => '${s.fromVersion}->${s.toVersion}').toList(), [
        '1->2',
        '2->3',
        '3->4',
      ]);
    });

    test('chain returns empty list when from == to', () {
      final reg = PromoterRegistry();
      final chain = reg.chain(
        viewName: 'v',
        entryType: 't',
        fromVersion: 1,
        toVersion: 1,
      );
      expect(chain, isEmpty);
    });

    test('chain throws when a step is missing', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      // missing 2 -> 3
      reg.register(_spec(3, 4));
      expect(
        () => reg.chain(
          viewName: 'v',
          entryType: 't',
          fromVersion: 1,
          toVersion: 4,
        ),
        throwsStateError,
      );
    });

    test('register throws on duplicate (view, entry, fromVersion)', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      expect(() => reg.register(_spec(1, 2)), throwsArgumentError);
    });

    test('register after seal throws', () {
      final reg = PromoterRegistry();
      reg.seal();
      expect(() => reg.register(_spec(1, 2)), throwsArgumentError);
    });
  });
}
