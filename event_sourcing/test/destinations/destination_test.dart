// Verifies: EVS-PRD-destinations/B/E
// exercises the Destination abstract
// interface: filter dispatching (B) and the app-supplied delivery contract
// (transform, send, SendResult variants — E).
import 'dart:typed_data';

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

/// Minimal concrete Destination used only to verify the abstract surface
/// type-checks and dispatches correctly.
class _EchoDestination extends Destination {
  _EchoDestination({required this.result});

  final SendResult result;
  final List<WirePayload> sent = [];

  @override
  String get id => 'echo';

  @override
  SubscriptionFilter get filter => const SubscriptionFilter();

  @override
  String get wireFormat => 'echo-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  // fixture admits at most one event per batch so the test
  // can assert the false-at-capacity branch.
  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async {
    if (batch.isEmpty) {
      throw ArgumentError('_EchoDestination.transform called with empty batch');
    }
    final joined = batch.map((e) => e.eventId).join(',');
    return WirePayload(
      bytes: Uint8List.fromList(joined.codeUnits),
      contentType: 'text/plain',
      transformVersion: 'echo-v1',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    return result;
  }
}

/// Destination that relies on the abstract-class default for
/// `allowHardDelete`. Used to verify its default-false contract
/// without the subclass overriding the getter.
class _DefaultDestination extends Destination {
  _DefaultDestination();

  @override
  String get id => 'defaults';

  @override
  SubscriptionFilter get filter => const SubscriptionFilter();

  @override
  String get wireFormat => 'defaults-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      false;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async {
    if (batch.isEmpty) {
      throw ArgumentError(
        '_DefaultDestination.transform called with empty batch',
      );
    }
    return WirePayload(
      bytes: Uint8List.fromList(const <int>[]),
      contentType: 'application/octet-stream',
      transformVersion: 'defaults-v1',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}

StoredEvent _mkEvent(String eventId) => StoredEvent(
  key: 1,
  eventId: eventId,
  aggregateId: 'agg-1',
  aggregateType: 'note',
  entryType: 'epistaxis_event',
  entryTypeVersion: 1,
  libFormatVersion: 1,
  eventType: 'finalized',
  sequenceNumber: 1,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u1'),
  clientTimestamp: DateTime.utc(2026, 4, 22),
  eventHash: 'hash',
);

void main() {
  group('Destination abstract contract', () {
    test('id and wireFormat are declared by the subclass', () {
      final dest = _EchoDestination(result: const SendOk());
      expect(dest.id, 'echo');
      expect(dest.wireFormat, 'echo-v1');
    });

    // WirePayload fields come straight from the subclass implementation. A
    // single-event batch is a batch of length one.
    test('transform returns subclass-produced WirePayload', () async {
      final dest = _EchoDestination(result: const SendOk());
      final payload = await dest.transform([_mkEvent('ev-abc')]);
      expect(payload.bytes, 'ev-abc'.codeUnits);
      expect(payload.contentType, 'text/plain');
      expect(payload.transformVersion, 'echo-v1');
    });

    test('send returns SendOk when scripted', () async {
      final dest = _EchoDestination(result: const SendOk());
      final payload = await dest.transform([_mkEvent('ev-1')]);
      final result = await dest.send(payload);
      expect(result, const SendOk());
      expect(dest.sent, hasLength(1));
      expect(dest.sent.single.bytes, 'ev-1'.codeUnits);
    });

    test('send returns SendTransient when scripted', () async {
      final dest = _EchoDestination(
        result: const SendTransient(error: 'HTTP 503', httpStatus: 503),
      );
      final result = await dest.send(await dest.transform([_mkEvent('ev-2')]));
      expect(result, isA<SendTransient>());
      expect((result as SendTransient).httpStatus, 503);
    });

    test('send returns SendPermanent when scripted', () async {
      final dest = _EchoDestination(
        result: const SendPermanent(error: 'HTTP 400'),
      );
      final result = await dest.send(await dest.transform([_mkEvent('ev-3')]));
      expect(result, isA<SendPermanent>());
      expect((result as SendPermanent).error, 'HTTP 400');
    });

    // subclass's SubscriptionFilter.
    test('filter dispatches to the subclass implementation', () {
      final dest = _EchoDestination(result: const SendOk());
      expect(dest.filter.matches(_mkEvent('ev-1')), isTrue);
    });

    // transform produces a single WirePayload covering every event in the
    // batch.
    test('Destination.transform(List<Event>) produces one '
        'WirePayload covering the whole batch', () async {
      final dest = _EchoDestination(result: const SendOk());
      final payload = await dest.transform([
        _mkEvent('ev-1'),
        _mkEvent('ev-2'),
        _mkEvent('ev-3'),
      ]);
      expect(payload.bytes, 'ev-1,ev-2,ev-3'.codeUnits);
      expect(payload.contentType, 'text/plain');
      expect(payload.transformVersion, 'echo-v1');
    });

    // empty batch; the subclass guards against it with ArgumentError.
    test('Destination.transform rejects empty batch with '
        'ArgumentError', () async {
      final dest = _EchoDestination(result: const SendOk());
      await expectLater(dest.transform(<StoredEvent>[]), throwsArgumentError);
    });

    // canAddToBatch returns true when the batch is empty and false once
    // the current batch is at capacity.
    test('canAddToBatch returns true when batch is empty and '
        'false once capacity is reached', () {
      final dest = _EchoDestination(result: const SendOk());
      // Capacity of this echo fixture is 1 — an empty batch accepts
      // the first candidate; a one-element batch refuses the next.
      expect(dest.canAddToBatch(<StoredEvent>[], _mkEvent('ev-1')), isTrue);
      expect(dest.canAddToBatch([_mkEvent('ev-1')], _mkEvent('ev-2')), isFalse);
    });

    // maxAccumulateTime is declared on the destination surface and defaults
    // to Duration.zero for this fixture.
    test('Destination.maxAccumulateTime is declared on the '
        'destination surface', () {
      final dest = _EchoDestination(result: const SendOk());
      expect(dest.maxAccumulateTime, Duration.zero);
    });

    // allowHardDelete defaults to false in the abstract contract so concrete
    // destinations must opt in explicitly.
    test('Destination.allowHardDelete defaults to false in the '
        'abstract contract', () {
      final dest = _DefaultDestination();
      expect(dest.allowHardDelete, isFalse);
    });
  });
}
