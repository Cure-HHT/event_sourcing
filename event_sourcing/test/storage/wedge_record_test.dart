// Verifies: EVS-PRD-destinations/Q
// the wedge cause has exactly three values,
//   each with its recorded string, and an unknown string is refused.
// Verifies: EVS-DEV-destination-drain/I
// the wedge record's persisted form
//   round-trips every field and refuses a malformed record.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('WedgeCause', () {
    test('has the three recorded causes', () {
      expect(
        <String, WedgeCause>{for (final c in WedgeCause.values) c.wire: c},
        <String, WedgeCause>{
          'permanent_refusal': WedgeCause.permanentRefusal,
          'retry_budget_exhausted': WedgeCause.retryBudgetExhausted,
          'operator_halt': WedgeCause.operatorHalt,
        },
      );
      for (final cause in WedgeCause.values) {
        expect(WedgeCause.fromWire(cause.wire), cause);
      }
    });

    test('refuses an unknown cause', () {
      expect(
        () => WedgeCause.fromWire('permanentRefusal'),
        throwsFormatException,
      );
      expect(() => WedgeCause.fromWire(''), throwsFormatException);
    });
  });

  group('WedgeRecord', () {
    test('round-trips every field', () {
      const minimal = WedgeRecord(
        rowId: 'r',
        wedgeEventId: 'e',
        cause: WedgeCause.retryBudgetExhausted,
      );
      const full = WedgeRecord(
        rowId: 'r',
        wedgeEventId: 'e',
        cause: WedgeCause.operatorHalt,
        haltPurpose: 'pause',
        drainerEpoch: 3,
        configurationFingerprint: 'fp',
      );
      for (final record in <WedgeRecord>[minimal, full]) {
        expect(WedgeRecord.fromJson(record.toJson()), record);
      }
      expect(minimal.toJson(), <String, Object?>{
        'row_id': 'r',
        'wedge_event_id': 'e',
        'cause': 'retry_budget_exhausted',
        'halt_purpose': null,
        'drainer_epoch': null,
        'configuration_fingerprint': null,
      });
      expect(minimal == full, isFalse);
    });

    test('refuses a malformed record', () {
      final valid = const WedgeRecord(
        rowId: 'r',
        wedgeEventId: 'e',
        cause: WedgeCause.permanentRefusal,
      ).toJson();
      for (final broken in <Map<String, Object?>>[
        {...valid}..remove('row_id'),
        {...valid, 'wedge_event_id': 1},
        {...valid, 'cause': 'unknown'},
        {...valid, 'halt_purpose': 2},
        {...valid, 'drainer_epoch': '3'},
        {...valid, 'configuration_fingerprint': 4},
      ]) {
        expect(
          () => WedgeRecord.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });
  });
}
