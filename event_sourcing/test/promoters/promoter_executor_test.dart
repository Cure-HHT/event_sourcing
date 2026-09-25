// Verifies: EVS-DEV-ingest-promotes-before-fold/A+B
// event_sourcing/test/promoters/promoter_executor_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('promotes payload through chain v1 -> v3', () {
    final reg = PromoterRegistry()
      ..register(
        const PromoterSpec(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(1, 0),
          toVersion: EntryTypeVersion(2, 0),
          transforms: [RenameField(sourceField: 'old', targetField: 'mid')],
        ),
      )
      ..register(
        const PromoterSpec(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(2, 0),
          toVersion: EntryTypeVersion(3, 0),
          transforms: [RenameField(sourceField: 'mid', targetField: 'final')],
        ),
      );
    final result = PromoterExecutor.promote(
      registry: reg,
      viewName: 'v',
      entryType: 't',
      fromVersion: const EntryTypeVersion(1, 0),
      toVersion: const EntryTypeVersion(3, 0),
      payload: const {'old': 'value'},
    );
    expect(result, {'final': 'value'});
  });

  test('returns input unchanged when from == to', () {
    final reg = PromoterRegistry();
    final result = PromoterExecutor.promote(
      registry: reg,
      viewName: 'v',
      entryType: 't',
      fromVersion: const EntryTypeVersion(2, 0),
      toVersion: const EntryTypeVersion(2, 0),
      payload: const {'a': 1},
    );
    expect(result, {'a': 1});
  });

  group('a promoted default is decided under its final name', () {
    // `1.0 -> 1.1` adds `x`; `1.1 -> 2.0` renames `x` to `y` and adds `z`;
    // `2.0 -> 3.0` drops `z`.
    PromoterRegistry registry() => PromoterRegistry()
      ..register(
        const PromoterSpec(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(1, 0),
          toVersion: EntryTypeVersion(1, 1),
          transforms: [DefaultField(fieldName: 'x', defaultValue: 0)],
        ),
      )
      ..register(
        const PromoterSpec(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(1, 1),
          toVersion: EntryTypeVersion(2, 0),
          transforms: [
            RenameField(sourceField: 'x', targetField: 'y'),
            DefaultField(fieldName: 'z', defaultValue: 'dz'),
          ],
        ),
      )
      ..register(
        const PromoterSpec(
          viewName: 'v',
          entryType: 't',
          fromVersion: EntryTypeVersion(2, 0),
          toVersion: EntryTypeVersion(3, 0),
          transforms: [DropField(fieldName: 'z')],
        ),
      );

    Map<String, Object?> promote(
      EntryTypeVersion to,
      Map<String, Object?> payload,
      Map<String, Object?>? existingRow,
    ) => PromoterExecutor.promote(
      registry: registry(),
      viewName: 'v',
      entryType: 't',
      fromVersion: const EntryTypeVersion(1, 0),
      toVersion: to,
      payload: payload,
      existingRow: existingRow,
    );

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('a default renamed later in the chain is skipped when the row holds '
        'the renamed field', () {
      expect(
        promote(
          const EntryTypeVersion(2, 0),
          const {'a': 3},
          const {'a': 1, 'y': 5},
        ),
        {'a': 3, 'z': 'dz'},
      );
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('a default renamed later in the chain is supplied, renamed, when '
        'the row lacks the renamed field', () {
      expect(
        promote(const EntryTypeVersion(2, 0), const {'a': 3}, const {'a': 1}),
        {'a': 3, 'y': 0, 'z': 'dz'},
      );
      expect(promote(const EntryTypeVersion(2, 0), const {'a': 3}, null), {
        'a': 3,
        'y': 0,
        'z': 'dz',
      });
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('a row holding the field under its old name does not suppress the '
        'renamed default', () {
      expect(
        promote(
          const EntryTypeVersion(2, 0),
          const {'a': 3},
          const {'a': 1, 'x': 5},
        ),
        {'a': 3, 'y': 0, 'z': 'dz'},
      );
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('a default dropped later in the chain has no effect', () {
      expect(
        promote(const EntryTypeVersion(3, 0), const {'a': 3}, const {'y': 5}),
        {'a': 3},
      );
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('an event that carries the field is renamed whatever the row '
        'holds', () {
      expect(
        promote(const EntryTypeVersion(2, 0), const {'x': 7}, const {'y': 5}),
        {'y': 7, 'z': 'dz'},
      );
    });
  });
}
