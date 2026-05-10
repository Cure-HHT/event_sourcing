// event_sourcing/test/promoters/promoter_executor_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('promotes payload through chain v1 -> v3', () {
    final reg = PromoterRegistry();
    reg.register(
      const PromoterSpec(
        viewName: 'v',
        entryType: 't',
        fromVersion: 1,
        toVersion: 2,
        transforms: [RenameField(from: 'old', to: 'mid')],
      ),
    );
    reg.register(
      const PromoterSpec(
        viewName: 'v',
        entryType: 't',
        fromVersion: 2,
        toVersion: 3,
        transforms: [RenameField(from: 'mid', to: 'final')],
      ),
    );
    final result = PromoterExecutor.promote(
      registry: reg,
      viewName: 'v',
      entryType: 't',
      fromVersion: 1,
      toVersion: 3,
      payload: const {'old': 'value'},
      firstEventTimestamp: DateTime.utc(2026, 1, 1),
    );
    expect(result, {'final': 'value'});
  });

  test('returns input unchanged when from == to', () {
    final reg = PromoterRegistry();
    final result = PromoterExecutor.promote(
      registry: reg,
      viewName: 'v',
      entryType: 't',
      fromVersion: 2,
      toVersion: 2,
      payload: const {'a': 1},
      firstEventTimestamp: DateTime.utc(2026, 1, 1),
    );
    expect(result, {'a': 1});
  });
}
