// Verifies: EVS-DEV-ingest-promotes-before-fold/C
// Verifies: EVS-DEV-snapshot-promotion-on-open/B
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';

const _default = DefaultField(fieldName: 'f', defaultValue: 0);
const _rename = RenameField(sourceField: 'a', targetField: 'b');
const _drop = DropField(fieldName: 'a');

PromoterSpec _spec(
  (int, int) from,
  (int, int) to, {
  List<TransformPrimitive> transforms = const <TransformPrimitive>[_default],
}) => PromoterSpec(
  viewName: 'v',
  entryType: 't',
  fromVersion: EntryTypeVersion(from.$1, from.$2),
  toVersion: EntryTypeVersion(to.$1, to.$2),
  transforms: transforms,
);

List<String> _steps(List<PromoterSpec> chain) => <String>[
  for (final s in chain) '${s.fromVersion}->${s.toVersion}',
];

List<PromoterSpec> _chain(
  PromoterRegistry reg,
  (int, int) from,
  (int, int) to,
) => reg.chain(
  viewName: 'v',
  entryType: 't',
  fromVersion: EntryTypeVersion(from.$1, from.$2),
  toVersion: EntryTypeVersion(to.$1, to.$2),
);

Matcher _argumentErrorNaming(String text) => throwsA(
  isA<ArgumentError>().having((e) => e.toString(), 'message', contains(text)),
);

void main() {
  group('PromoterRegistry.register', () {
    // Verifies: EVS-DEV-version-compatibility/B
    test('a minor step whose transforms are DefaultField is accepted', () {
      final reg = PromoterRegistry()..register(_spec((1, 0), (1, 1)));
      expect(_steps(_chain(reg, (1, 0), (1, 1))), <String>['1.0->1.1']);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a minor step with RenameField is refused, naming the rule', () {
      final reg = PromoterRegistry();
      expect(
        () => reg.register(
          _spec(
            (1, 0),
            (1, 1),
            transforms: const <TransformPrimitive>[_default, _rename],
          ),
        ),
        _argumentErrorNaming('minor step is compatible by definition'),
      );
      expect(() => _chain(reg, (1, 0), (2, 0)), throwsStateError);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a minor step with DropField is refused, naming the rule', () {
      final reg = PromoterRegistry();
      expect(
        () => reg.register(
          _spec((1, 2), (1, 3), transforms: const <TransformPrimitive>[_drop]),
        ),
        _argumentErrorNaming('RenameField and DropField require a major step'),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a step that skips a minor is refused', () {
      final reg = PromoterRegistry();
      expect(
        () => reg.register(_spec((1, 0), (1, 2))),
        _argumentErrorNaming('next minor'),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a step to a lower or equal version is refused', () {
      final reg = PromoterRegistry();
      expect(() => reg.register(_spec((1, 2), (1, 2))), throwsArgumentError);
      expect(() => reg.register(_spec((2, 0), (1, 5))), throwsArgumentError);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a major step to minor 0 of the next major may rename and drop', () {
      final reg = PromoterRegistry()
        ..register(
          _spec(
            (1, 2),
            (2, 0),
            transforms: const <TransformPrimitive>[_rename, _drop],
          ),
        );
      expect(_steps(_chain(reg, (1, 2), (2, 0))), <String>['1.2->2.0']);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a major step to a minor above 0 is refused', () {
      final reg = PromoterRegistry();
      expect(
        () => reg.register(
          _spec(
            (1, 2),
            (2, 1),
            transforms: const <TransformPrimitive>[_rename],
          ),
        ),
        _argumentErrorNaming('minor 0 of the next major'),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a step across more than one major is refused', () {
      final reg = PromoterRegistry();
      expect(
        () => reg.register(_spec((1, 2), (3, 0))),
        _argumentErrorNaming('minor 0 of the next major'),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a second major step from one major is refused', () {
      final reg = PromoterRegistry()
        ..register(
          _spec(
            (1, 2),
            (2, 0),
            transforms: const <TransformPrimitive>[_rename],
          ),
        );
      expect(
        () => reg.register(
          _spec((1, 4), (2, 0), transforms: const <TransformPrimitive>[_drop]),
        ),
        _argumentErrorNaming('second major step'),
      );
      // The registered step is unchanged.
      expect(_steps(_chain(reg, (1, 2), (2, 0))), <String>['1.2->2.0']);
    });

    test('register throws on a duplicate (view, entry type, from)', () {
      final reg = PromoterRegistry()..register(_spec((1, 0), (1, 1)));
      expect(() => reg.register(_spec((1, 0), (1, 1))), throwsArgumentError);
    });

    test('register after seal throws', () {
      final reg = PromoterRegistry()..seal();
      expect(() => reg.register(_spec((1, 0), (1, 1))), throwsArgumentError);
    });
  });

  group('PromoterRegistry.chain', () {
    // Verifies: EVS-DEV-version-compatibility/B
    test('a missing minor step is the identity', () {
      final reg = PromoterRegistry()..register(_spec((1, 1), (1, 2)));
      expect(_steps(_chain(reg, (1, 0), (1, 3))), <String>['1.1->1.2']);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('walks missing minors up to the major step, then the new major', () {
      final reg = PromoterRegistry()
        ..register(
          _spec(
            (1, 2),
            (2, 0),
            transforms: const <TransformPrimitive>[_rename],
          ),
        )
        ..register(_spec((2, 0), (2, 1)));
      expect(_steps(_chain(reg, (1, 1), (2, 1))), <String>[
        '1.2->2.0',
        '2.0->2.1',
      ]);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a version past the major step of its major throws', () {
      final reg = PromoterRegistry()
        ..register(
          _spec(
            (1, 2),
            (2, 0),
            transforms: const <TransformPrimitive>[_rename],
          ),
        );
      expect(
        () => _chain(reg, (1, 3), (2, 0)),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('1.3'),
          ),
        ),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a major with no registered major step throws', () {
      final reg = PromoterRegistry();
      expect(() => _chain(reg, (1, 0), (2, 0)), throwsStateError);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a higher minor of the target major returns the empty chain', () {
      final reg = PromoterRegistry()..register(_spec((2, 0), (2, 1)));
      expect(_chain(reg, (2, 1), (2, 0)), isEmpty);
      expect(_chain(reg, (2, 1), (2, 1)), isEmpty);
    });

    // Verifies: EVS-DEV-version-compatibility/B
    test('a higher major than the target throws', () {
      final reg = PromoterRegistry();
      expect(() => _chain(reg, (3, 0), (2, 5)), throwsStateError);
    });
  });

  group('chainGap', () {
    String? gap(PromoterRegistry reg, (int, int) from, (int, int) to) =>
        reg.chainGap(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(from.$1, from.$2),
          toVersion: EntryTypeVersion(to.$1, to.$2),
        );

    // Verifies: EVS-DEV-version-compatibility/D
    test('is null exactly where chain returns a chain, and names the missing '
        'step where chain throws', () {
      final reg = PromoterRegistry()
        ..register(
          _spec(
            (1, 2),
            (2, 0),
            transforms: const <TransformPrimitive>[
              RenameField(sourceField: 'a', targetField: 'b'),
            ],
          ),
        );
      expect(gap(reg, (1, 0), (2, 0)), isNull);
      expect(gap(reg, (2, 1), (2, 0)), isNull);
      expect(gap(reg, (1, 3), (2, 0)), contains('past the major step'));
      expect(gap(PromoterRegistry(), (1, 0), (2, 0)), contains('major 1'));
      expect(gap(reg, (3, 0), (2, 0)), contains('lower major'));
    });
  });
}
