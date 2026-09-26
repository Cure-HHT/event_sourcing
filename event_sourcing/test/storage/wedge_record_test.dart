// Verifies: EVS-PRD-destinations/Q
// this build wedges with exactly its four causes (a permanent failure,
//   an exhausted retry budget, an operator halt and an acceptance carrying
//   no receiver record), each with its recorded string.
// Verifies: EVS-DEV-destination-drain/I
// the wedge record's persisted form
//   round-trips every field and refuses a malformed record.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('WedgeCause', () {
    test('has the four recorded causes', () {
      expect(
        <String, WedgeCause>{for (final c in WedgeCause.values) c.wire: c},
        <String, WedgeCause>{
          'permanent_refusal': WedgeCause.permanentRefusal,
          'retry_budget_exhausted': WedgeCause.retryBudgetExhausted,
          'operator_halt': WedgeCause.operatorHalt,
          'acknowledgement_invalid': WedgeCause.acknowledgementInvalid,
        },
      );
      for (final cause in WedgeCause.values) {
        expect(WedgeCause.fromWire(cause.wire), cause);
      }
    });

    // Verifies: EVS-DEV-destination-drain/L
    // a cause this build does not know is carried verbatim, equal to itself
    //   and to no cause it knows.
    test('carries an unknown cause verbatim', () {
      final future = WedgeCause.fromWire('future_cause');
      expect(future.wire, 'future_cause');
      expect(future.isKnown, isFalse);
      expect(future, WedgeCause.fromWire('future_cause'));
      expect(future.hashCode, WedgeCause.fromWire('future_cause').hashCode);
      expect(
        WedgeCause.fromWire('permanentRefusal'),
        isNot(WedgeCause.permanentRefusal),
      );
      expect(WedgeCause.values, isNot(contains(future)));
      for (final cause in WedgeCause.values) {
        expect(cause.isKnown, isTrue);
        expect(WedgeCause.fromWire(cause.wire), same(cause));
      }
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
        haltPurpose: HaltPurpose.pause,
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
      expect(full.toJson()['halt_purpose'], 'pause');
      expect(minimal == full, isFalse);
    });

    // Verifies: EVS-DEV-destination-drain/L
    // a wedge record a newer build of the major wrote, with a cause and a
    //   halt purpose this build does not know, reads back verbatim.
    test('reads a record of an unknown cause and purpose back verbatim', () {
      final json = <String, Object?>{
        'row_id': 'r',
        'wedge_event_id': 'e',
        'cause': 'future_cause',
        'halt_purpose': 'future_purpose',
        'drainer_epoch': 2,
        'configuration_fingerprint': 'fp',
      };
      final read = WedgeRecord.fromJson(json);
      expect(read.cause.wire, 'future_cause');
      expect(read.haltPurpose!.wire, 'future_purpose');
      expect(read.toJson(), json);
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
        {...valid, 'cause': 7},
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
