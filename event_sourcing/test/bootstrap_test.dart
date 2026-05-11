import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'test_support/fake_destination.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'bootstrap-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'd',
  softwareVersion: 'v',
);

EntryTypeDefinition _defn(String id) =>
    EntryTypeDefinition(id: id, registeredVersion: 1, name: id);

/// Destination that throws on the first read of [id]. Used to abort the
/// destination loop at a deterministic point.
class _ThrowOnIdAccess extends FakeDestination {
  _ThrowOnIdAccess() : super(id: 'unused', script: const []);

  @override
  String get id => throw StateError('id getter intentionally throws');
}

void main() {
  group('bootstrapAppendOnlyDatastore', () {
    test(
      'REQ-d00134-A: returns AppendOnlyDatastore facade carrying eventStore, '
      'entryTypes, destinations, securityContexts',
      () async {
        final backend = await _openBackend();
        final ds = await bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: [_defn('demo_note')],
          destinations: const <Destination>[],
        );
        expect(ds.eventStore, isA<EventStore>());
        expect(ds.entryTypes, isA<EntryTypeRegistry>());
        expect(ds.destinations, isA<DestinationRegistry>());
        expect(ds.securityContexts, isA<SecurityContextStore>());
        expect(ds.entryTypes.isRegistered('demo_note'), isTrue);
      },
    );

    test('REQ-d00134-B: auto-registers 3 reserved system entry types BEFORE '
        'caller-supplied list', () async {
      final backend = await _openBackend();
      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: [_defn('demo_note')],
        destinations: const <Destination>[],
      );
      expect(ds.entryTypes.isRegistered('security_context_redacted'), isTrue);
      expect(ds.entryTypes.isRegistered('security_context_compacted'), isTrue);
      expect(ds.entryTypes.isRegistered('security_context_purged'), isTrue);
    });

    test('REQ-d00134-D: caller-supplied id colliding with reserved id throws '
        'ArgumentError with "reserved" message', () async {
      final backend = await _openBackend();
      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: [_defn('security_context_redacted')],
          destinations: const <Destination>[],
        ),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message.toString(),
            'message',
            contains('reserved'),
          ),
        ),
      );
    });

    test('REQ-d00134-A+C: wires entry types and destinations; registry stays '
        'open', () async {
      final backend = await _openBackend();
      final types = [_defn('demo_note'), _defn('red_button')];
      final dests = [
        FakeDestination(id: 'primary', script: const []),
        FakeDestination(id: 'analytics', script: const []),
      ];

      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: types,
        destinations: dests,
      );

      // 2 caller-supplied + 14 system = 16 total
      expect(ds.entryTypes.all(), hasLength(16));
      expect(ds.entryTypes.isRegistered('demo_note'), isTrue);
      expect(ds.entryTypes.isRegistered('red_button'), isTrue);
      expect(ds.destinations.all(), hasLength(2));
      expect(ds.destinations.byId('primary'), same(dests[0]));
      expect(ds.destinations.byId('analytics'), same(dests[1]));
      expect(await backend.readSchedule('primary'), isA<DestinationSchedule>());

      // Registry remains open to subsequent runtime addDestination calls.
      await ds.destinations.addDestination(
        FakeDestination(id: 'late', script: const []),
        initiator: const AutomationInitiator(service: 'test-bootstrap'),
      );
      expect(ds.destinations.all(), hasLength(3));
    });

    test('REQ-d00134-B: type-loop runs first — duplicate type id throws '
        'before any destination is registered', () async {
      final backend = await _openBackend();
      final types = [_defn('dup'), _defn('dup')];
      final dests = [FakeDestination(id: 'primary', script: const [])];

      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: types,
          destinations: dests,
        ),
        throwsArgumentError,
      );
      expect(await backend.readSchedule('primary'), isNull);
    });

    test('REQ-d00134-B: when the destination loop throws, supplied types were '
        'registered first (ordering proof)', () async {
      final backend = await _openBackend();
      final types = [_defn('demo_note'), _defn('red_button')];
      final dests = <Destination>[_ThrowOnIdAccess()];

      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: types,
          destinations: dests,
        ),
        throwsA(isA<StateError>()),
      );
      expect(await backend.readSchedule('unused'), isNull);

      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: types,
        destinations: const <Destination>[],
      );
      expect(ds.entryTypes.isRegistered('demo_note'), isTrue);
      expect(ds.entryTypes.isRegistered('red_button'), isTrue);
    });

    test('REQ-d00134-D: duplicate destination id throws', () async {
      final backend = await _openBackend();
      final dests = [
        FakeDestination(id: 'x', script: const []),
        FakeDestination(id: 'x', script: const []),
      ];

      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: const [],
          destinations: dests,
        ),
        throwsArgumentError,
      );
    });

    test(
      'REQ-d00134-D: id collision after a successful first registration '
      'leaves the first destination persisted (sequential registration)',
      () async {
        final backend = await _openBackend();
        final dests = [
          FakeDestination(id: 'first', script: const []),
          FakeDestination(id: 'second', script: const []),
          FakeDestination(id: 'second', script: const []),
        ];

        await expectLater(
          bootstrapAppendOnlyDatastore(
            backend: backend,
            source: _source,
            entryTypes: const [],
            destinations: dests,
          ),
          throwsArgumentError,
        );
        expect(await backend.readSchedule('first'), isNotNull);
        expect(await backend.readSchedule('second'), isNotNull);
      },
    );
  });

  // -------------------------------------------------------------------------
  // Boot-version check fires through the production bootstrap path
  // Verifies: EVS-DEV-event-store-open (via bootstrapAppendOnlyDatastore) —
  //   the production entry point routes through EventStore.open so the
  //   lib-version check fires in real app boots, not only in direct
  //   EventStore.open calls.
  // -------------------------------------------------------------------------
  group('bootstrapAppendOnlyDatastore routes through EventStore.open', () {
    test('emits lib_version_initialized on first bootstrap', () async {
      final backend = await _openBackend();
      await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[],
        destinations: const <Destination>[],
      );
      final result = await VersionCheck.findMostRecent(backend);
      expect(result, isNotNull);
      expect(result!.recordedVersion, LibVersion.current);
      expect(result.eventType, LibVersionEvents.initialized);
    });

    test(
      'throws DowngradeRefusedError when log was written by a newer lib',
      () async {
        final backend = await _openBackend();
        // Simulate a future-version boot by directly appending a synthetic
        // lib_version_initialized event with version 99.0.0.
        await backend.transaction((txn) async {
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(
            txn,
            StoredEvent.synthetic(
              eventId: 'synth-future-$seq',
              aggregateId: '_lib',
              aggregateType: '_lib',
              entryType: LibVersionEvents.initialized,
              eventType: LibVersionEvents.initialized,
              sequenceNumber: seq,
              eventHash: 'h-$seq',
              initiator: const AutomationInitiator(service: 'event_sourcing'),
              clientTimestamp: DateTime.utc(2026, 5, 9),
              data: <String, dynamic>{
                'version': '99.0.0',
                'initializedAt': '2026-05-09T00:00:00Z',
              },
            ),
          );
        });
        await expectLater(
          bootstrapAppendOnlyDatastore(
            backend: backend,
            source: _source,
            entryTypes: const <EntryTypeDefinition>[],
            destinations: const <Destination>[],
          ),
          throwsA(isA<DowngradeRefusedError>()),
        );
      },
    );

    test(
      'allowDowngrade: true bypasses downgrade refusal in bootstrap path',
      () async {
        final backend = await _openBackend();
        await backend.transaction((txn) async {
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(
            txn,
            StoredEvent.synthetic(
              eventId: 'synth-future-$seq',
              aggregateId: '_lib',
              aggregateType: '_lib',
              entryType: LibVersionEvents.initialized,
              eventType: LibVersionEvents.initialized,
              sequenceNumber: seq,
              eventHash: 'h-$seq',
              initiator: const AutomationInitiator(service: 'event_sourcing'),
              clientTimestamp: DateTime.utc(2026, 5, 9),
              data: <String, dynamic>{
                'version': '99.0.0',
                'initializedAt': '2026-05-09T00:00:00Z',
              },
            ),
          );
        });
        // allowDowngrade: true — should succeed despite 99.0.0 recorded version.
        final ds = await bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[],
          destinations: const <Destination>[],
          allowDowngrade: true,
        );
        expect(ds.eventStore, isA<EventStore>());
      },
    );
  });
}
