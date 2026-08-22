// Verifies: EVS-PRD-portability/C
// FifoEntry pure-Dart value type;
//   toJson/fromJson round-trips produce identical results on any runtime;
//   constructor invariants (non-empty eventIds, firstSeq <= lastSeq) are
//   enforced in both release and debug builds.
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:flutter_test/flutter_test.dart';

/// Verifies the FifoEntry shape:
///
/// - `eventIds` is a non-empty `List<String>`.
/// - `sequenceRange` is a (firstSeq, lastSeq) record drawn from
///   the sequence_numbers of the contained events.
/// - `wirePayload` is one payload covering the whole batch;
///   no per-event wire payload is stored.
/// - `FinalStatus` has exactly `{sent, wedged, tombstoned}`.
/// - `FifoEntry.finalStatus` is nullable; `null` means "not-yet-terminal".
void main() {
  final enqueuedAt = DateTime.utc(2026, 4, 22, 9);

  FifoEntry makeBatch({
    List<String>? eventIds,
    SequenceRange? sequenceRange,
    Map<String, Object?>? wirePayload,
  }) {
    return FifoEntry(
      entryId: 'entry-1',
      eventIds: eventIds ?? const ['ev-1', 'ev-2', 'ev-3'],
      sequenceRange: sequenceRange ?? (firstSeq: 10, lastSeq: 12),
      sequenceInQueue: 1,
      wirePayload: wirePayload ?? const <String, Object?>{'batch': 'ok'},
      wireFormat: 'json-v1',
      transformVersion: 'transform-v1',
      enqueuedAt: enqueuedAt,
      attempts: const <AttemptResult>[],
      finalStatus: null,
      sentAt: null,
    );
  }

  group('FifoEntry batch shape', () {
    // eventIds is a non-empty List<String> identifying every event in the batch.
    test('eventIds is a non-empty List<String> with every batch '
        'event id', () {
      final entry = makeBatch(eventIds: const ['ev-a', 'ev-b', 'ev-c']);
      expect(entry.eventIds, isA<List<String>>());
      expect(entry.eventIds, ['ev-a', 'ev-b', 'ev-c']);
      expect(entry.eventIds, isNotEmpty);
    });

    // The empty-eventIds invariant is enforced at construction so a batch row
    // can never be persisted with zero events.
    test('constructing FifoEntry with empty eventIds throws', () {
      expect(
        () => FifoEntry(
          entryId: 'entry-1',
          eventIds: const <String>[],
          sequenceRange: (firstSeq: 0, lastSeq: 0),
          sequenceInQueue: 1,
          wirePayload: const <String, Object?>{},
          wireFormat: 'json-v1',
          transformVersion: null,
          enqueuedAt: enqueuedAt,
          attempts: const <AttemptResult>[],
          finalStatus: null,
          sentAt: null,
        ),
        throwsArgumentError,
      );
    });

    // (firstSeq <= lastSeq) is enforced at runtime, not just by convention.
    test('constructing FifoEntry with firstSeq > lastSeq throws', () {
      expect(
        () => FifoEntry(
          entryId: 'entry-1',
          eventIds: const <String>['e1'],
          sequenceRange: (firstSeq: 5, lastSeq: 3),
          sequenceInQueue: 1,
          wirePayload: const <String, Object?>{},
          wireFormat: 'json-v1',
          transformVersion: null,
          enqueuedAt: enqueuedAt,
          attempts: const <AttemptResult>[],
          finalStatus: null,
          sentAt: null,
        ),
        throwsArgumentError,
      );
    });

    // fromJson enforces the eventIds invariant: missing, non-List, and
    // empty event_ids fields all throw FormatException.
    test('fromJson rejects missing, non-List, and empty '
        'event_ids', () {
      final base = makeBatch().toJson();

      final missing = Map<String, Object?>.from(base)..remove('event_ids');
      expect(() => FifoEntry.fromJson(missing), throwsFormatException);

      final wrongType = Map<String, Object?>.from(base)
        ..['event_ids'] = 'not-a-list';
      expect(() => FifoEntry.fromJson(wrongType), throwsFormatException);

      final empty = Map<String, Object?>.from(base)..['event_ids'] = <String>[];
      expect(() => FifoEntry.fromJson(empty), throwsFormatException);
    });

    // sequenceRange is drawn from the sequence_number values of the contained
    // events; the pair is typed as SequenceRange (Dart 3 record typedef).
    test('sequenceRange is an (firstSeq, lastSeq) record', () {
      final entry = makeBatch(sequenceRange: (firstSeq: 100, lastSeq: 104));
      expect(entry.sequenceRange.firstSeq, 100);
      expect(entry.sequenceRange.lastSeq, 104);
      expect(entry.sequenceRange, isA<SequenceRange>());
      // Record equality is structural.
      expect(entry.sequenceRange, (firstSeq: 100, lastSeq: 104));
    });

    // event_id_range persists as {"first_seq": int, "last_seq": int} in JSON.
    test('event_id_range persists as first_seq/last_seq Map', () {
      final entry = makeBatch(sequenceRange: (firstSeq: 7, lastSeq: 9));
      final json = entry.toJson();
      expect(
        json['event_id_range'],
        equals(<String, Object?>{'first_seq': 7, 'last_seq': 9}),
      );
      final decoded = FifoEntry.fromJson(json);
      expect(decoded.sequenceRange, (firstSeq: 7, lastSeq: 9));
    });

    // fromJson enforces the event_id_range field shape.
    test('fromJson rejects missing or malformed event_id_range', () {
      final base = makeBatch().toJson();

      final missing = Map<String, Object?>.from(base)..remove('event_id_range');
      expect(() => FifoEntry.fromJson(missing), throwsFormatException);

      final wrongType = Map<String, Object?>.from(base)
        ..['event_id_range'] = 'oops';
      expect(() => FifoEntry.fromJson(wrongType), throwsFormatException);

      final missingFirst = Map<String, Object?>.from(base)
        ..['event_id_range'] = <String, Object?>{'last_seq': 5};
      expect(() => FifoEntry.fromJson(missingFirst), throwsFormatException);

      final missingLast = Map<String, Object?>.from(base)
        ..['event_id_range'] = <String, Object?>{'first_seq': 5};
      expect(() => FifoEntry.fromJson(missingLast), throwsFormatException);

      final nonIntSeq = Map<String, Object?>.from(base)
        ..['event_id_range'] = <String, Object?>{
          'first_seq': 1,
          'last_seq': '2',
        };
      expect(() => FifoEntry.fromJson(nonIntSeq), throwsFormatException);
    });

    // wirePayload is a single map covering the whole batch; no per-event
    // payload is stored.
    test('wirePayload is a single map covering the whole batch', () {
      final entry = makeBatch(
        eventIds: const ['ev-a', 'ev-b'],
        wirePayload: const <String, Object?>{
          'events': [
            <String, Object?>{'id': 'ev-a'},
            <String, Object?>{'id': 'ev-b'},
          ],
        },
      );
      expect(entry.wirePayload, isA<Map<String, Object?>>());
      expect(entry.wirePayload!['events'], isA<List<Object?>>());
      expect((entry.wirePayload!['events']! as List).length, 2);
    });

    // The batch shape round-trips through toJson/fromJson. The legacy single-
    // event 'event_id' scalar key is NOT emitted.
    test('JSON round-trip preserves new shape and does not '
        'emit legacy event_id scalar', () {
      final entry = makeBatch(
        eventIds: const ['ev-a', 'ev-b'],
        sequenceRange: (firstSeq: 20, lastSeq: 21),
      );
      final json = entry.toJson();
      expect(json.containsKey('event_id'), isFalse);
      expect(json['event_ids'], ['ev-a', 'ev-b']);
      expect(json['event_id_range'], <String, Object?>{
        'first_seq': 20,
        'last_seq': 21,
      });
      final decoded = FifoEntry.fromJson(json);
      expect(decoded.eventIds, ['ev-a', 'ev-b']);
      expect(decoded.sequenceRange, (firstSeq: 20, lastSeq: 21));
      expect(decoded, equals(entry));
    });

    // Verifies: equality still covers the new eventIds and sequenceRange
    // fields; two entries that differ only in eventIds are NOT equal.
    test('equality distinguishes entries differing only in eventIds', () {
      final a = makeBatch(eventIds: const ['ev-x']);
      final b = makeBatch(eventIds: const ['ev-y']);
      expect(a, isNot(equals(b)));
    });

    test('equality distinguishes entries differing only in sequenceRange', () {
      final a = makeBatch(sequenceRange: (firstSeq: 1, lastSeq: 1));
      final b = makeBatch(sequenceRange: (firstSeq: 2, lastSeq: 2));
      expect(a, isNot(equals(b)));
    });

    test('parsed eventIds list is unmodifiable', () {
      final decoded = FifoEntry.fromJson(makeBatch().toJson());
      expect(() => decoded.eventIds.add('x'), throwsUnsupportedError);
    });
  });

  group('FifoEntry nullable final_status', () {
    // null means "not yet terminal". Drain may attempt a row whose
    // finalStatus is null; non-null terminal values are retained forever
    // as audit records.
    test('finalStatus is nullable; null is not a terminal state', () {
      final entry = FifoEntry(
        entryId: 'e1',
        eventIds: const ['ev1'],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
        sequenceInQueue: 1,
        wirePayload: const {'k': 'v'},
        wireFormat: 'json-v1',
        transformVersion: 'v1',
        enqueuedAt: DateTime.utc(2026, 4, 23, 10),
        attempts: const [],
        finalStatus: null,
        sentAt: null,
      );
      expect(entry.finalStatus, isNull);
    });

    // FinalStatus has exactly three values; `pending` and `exhausted` are
    // not valid values and are rejected by fromJson.
    test('FinalStatus enum has exactly {sent, wedged, '
        'tombstoned}', () {
      expect(FinalStatus.values.toSet(), {
        FinalStatus.sent,
        FinalStatus.wedged,
        FinalStatus.tombstoned,
      });
    });

    // fromJson rejects the strings 'pending' and 'exhausted' so a stale
    // persisted row with an old value cannot quietly load as something else.
    test('FinalStatus.fromJson rejects pending and exhausted', () {
      expect(() => FinalStatus.fromJson('pending'), throwsFormatException);
      expect(() => FinalStatus.fromJson('exhausted'), throwsFormatException);
      expect(FinalStatus.fromJson('sent'), FinalStatus.sent);
      expect(FinalStatus.fromJson('wedged'), FinalStatus.wedged);
      expect(FinalStatus.fromJson('tombstoned'), FinalStatus.tombstoned);
    });

    // fromJson accepts a null final_status and round-trips it through
    // toJson as explicit null (not omission).
    test('FifoEntry.fromJson accepts null final_status', () {
      final json = {
        'entry_id': 'e1',
        'event_ids': ['ev1'],
        'event_id_range': {'first_seq': 1, 'last_seq': 1},
        'sequence_in_queue': 1,
        'wire_payload': {'k': 'v'},
        'wire_format': 'json-v1',
        'transform_version': 'v1',
        'enqueued_at': '2026-04-23T10:00:00.000Z',
        'attempts': <Map<String, Object?>>[],
        'final_status': null,
        'sent_at': null,
      };
      final entry = FifoEntry.fromJson(json);
      expect(entry.finalStatus, isNull);
      expect(entry.toJson()['final_status'], isNull);
    });
  });

  group('FifoEntry envelopeMetadata + nullable wirePayload', () {
    final meta = BatchEnvelopeMetadata(
      batchFormatVersion: '1',
      batchId: 'b-001',
      senderHop: 'mobile-1',
      senderIdentifier: 'device-uuid',
      senderSoftwareVersion: 'diary@1.2.3',
      sentAt: DateTime.utc(2026, 4, 25, 12),
    );

    // A native row carries envelopeMetadata and a null wire_payload, and
    // the pair round-trips through toJson / fromJson with both fields preserved.
    test('native row round-trips envelopeMetadata with null '
        'wirePayload', () {
      final entry = FifoEntry(
        entryId: 'fe-001',
        eventIds: const <String>['e1'],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
        sequenceInQueue: 1,
        wireFormat: 'esd/batch@1',
        wirePayload: null,
        transformVersion: 'native-v1',
        enqueuedAt: DateTime.utc(2026, 4, 25, 12),
        attempts: const <AttemptResult>[],
        finalStatus: null,
        sentAt: null,
        envelopeMetadata: meta,
      );
      expect(entry.wirePayload, isNull);
      expect(entry.envelopeMetadata, meta);

      final json = entry.toJson();
      expect(json['wire_payload'], isNull);
      expect(
        json['envelope_metadata'],
        equals(<String, Object?>{
          'batch_format_version': '1',
          'batch_id': 'b-001',
          'sender_hop': 'mobile-1',
          'sender_identifier': 'device-uuid',
          'sender_software_version': 'diary@1.2.3',
          'sent_at': '2026-04-25T12:00:00.000Z',
        }),
      );

      final restored = FifoEntry.fromJson(json);
      expect(restored.wirePayload, isNull);
      expect(restored.envelopeMetadata, meta);
      expect(restored, equals(entry));
    });

    // A 3rd-party row carries a non-null wire_payload and a null
    // envelope_metadata, and the pair round-trips through toJson / fromJson
    // with both fields preserved.
    test('3rd-party row round-trips wirePayload with null '
        'envelopeMetadata', () {
      final entry = FifoEntry(
        entryId: 'fe-002',
        eventIds: const <String>['e1'],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
        sequenceInQueue: 1,
        wireFormat: 'application/csv',
        wirePayload: const <String, Object?>{
          'rows': <Object?>[
            <String, Object?>{'a': 1, 'b': 2},
          ],
        },
        transformVersion: 'csv-v1',
        enqueuedAt: DateTime.utc(2026, 4, 25, 12),
        attempts: const <AttemptResult>[],
        finalStatus: null,
        sentAt: null,
        envelopeMetadata: null,
      );
      expect(entry.wirePayload, isNotNull);
      expect(entry.envelopeMetadata, isNull);

      final json = entry.toJson();
      expect(json['wire_payload'], isNotNull);
      expect(json['envelope_metadata'], isNull);

      final restored = FifoEntry.fromJson(json);
      expect(restored.wirePayload, isNotNull);
      expect(restored.wirePayload!['rows'], isA<List<Object?>>());
      expect(restored.envelopeMetadata, isNull);
      expect(restored, equals(entry));
    });
  });
}
