import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'test_support/fake_destination.dart';
import 'test_support/lib_version_seed.dart';

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

EntryTypeDefinition _defn(String id) => EntryTypeDefinition(
  id: id,
  registeredVersion: const EntryTypeVersion(1, 0),
  name: id,
);

/// Destination that throws on the first read of [id]. Used to abort the
/// destination loop at a deterministic point.
class _ThrowOnIdAccess extends FakeDestination {
  _ThrowOnIdAccess() : super(id: 'unused', script: const []);

  @override
  String get id => throw StateError('id getter intentionally throws');
}

void main() {
  group('bootstrapEventStore', () {
    test('returns EventStoreBundle facade carrying eventStore, '
        'entryTypes, destinations, securityContexts', () async {
      final backend = await _openBackend();
      final ds = await bootstrapEventStore(
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
    });

    test('registers the reserved system entry types beside the '
        'caller-supplied list', () async {
      final backend = await _openBackend();
      final ds = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: [_defn('demo_note')],
        destinations: const <Destination>[],
      );
      expect(ds.entryTypes.isRegistered('security_context_redacted'), isTrue);
      expect(ds.entryTypes.isRegistered('security_context_compacted'), isTrue);
      expect(ds.entryTypes.isRegistered('security_context_purged'), isTrue);
    });

    test('caller-supplied id colliding with reserved id throws '
        'ArgumentError with "reserved" message', () async {
      final backend = await _openBackend();
      await expectLater(
        bootstrapEventStore(
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

    test('wires entry types and destinations; registry stays '
        'open', () async {
      final backend = await _openBackend();
      final types = [_defn('demo_note'), _defn('red_button')];
      final dests = [
        FakeDestination(id: 'primary', script: const []),
        FakeDestination(id: 'analytics', script: const []),
      ];

      final ds = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: types,
        destinations: dests,
      );

      // 2 caller-supplied + 17 system = 19 total
      expect(ds.entryTypes.all(), hasLength(19));
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

    test('type-loop runs first — duplicate type id throws '
        'before any destination is registered', () async {
      final backend = await _openBackend();
      final types = [_defn('dup'), _defn('dup')];
      final dests = [FakeDestination(id: 'primary', script: const [])];

      await expectLater(
        bootstrapEventStore(
          backend: backend,
          source: _source,
          entryTypes: types,
          destinations: dests,
        ),
        throwsArgumentError,
      );
      expect(await backend.readSchedule('primary'), isNull);
    });

    test('when the destination loop throws, supplied types were '
        'registered first (ordering proof)', () async {
      final backend = await _openBackend();
      final types = [_defn('demo_note'), _defn('red_button')];
      final dests = <Destination>[_ThrowOnIdAccess()];

      await expectLater(
        bootstrapEventStore(
          backend: backend,
          source: _source,
          entryTypes: types,
          destinations: dests,
        ),
        throwsA(isA<StateError>()),
      );
      expect(await backend.readSchedule('unused'), isNull);

      // The failed call itself recorded the registry audit, which only the
      // type step writes and which lists every registered type, before the
      // destination loop threw: the supplied types were registered first.
      final audits = await backend.findAllEvents(
        entryType: kEntryTypeRegistryInitializedEntryType,
      );
      expect(audits, hasLength(1));
      final registry = audits.single.data['registry']! as Map;
      expect(registry['demo_note'], '1.0');
      expect(registry['red_button'], '1.0');
      // Nothing of the destination step was written.
      final all = await backend.findAllEvents();
      expect(all.last.eventId, audits.single.eventId);
    });

    test('duplicate destination id throws', () async {
      final backend = await _openBackend();
      final dests = [
        FakeDestination(id: 'x', script: const []),
        FakeDestination(id: 'x', script: const []),
      ];

      await expectLater(
        bootstrapEventStore(
          backend: backend,
          source: _source,
          entryTypes: const [],
          destinations: dests,
        ),
        throwsArgumentError,
      );
    });

    test(
      'id collision after a successful first registration '
      'leaves the first destination persisted (sequential registration)',
      () async {
        final backend = await _openBackend();
        final dests = [
          FakeDestination(id: 'first', script: const []),
          FakeDestination(id: 'second', script: const []),
          FakeDestination(id: 'second', script: const []),
        ];

        await expectLater(
          bootstrapEventStore(
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
  // Verifies: EVS-DEV-event-store-open
  // (via bootstrapEventStore) —
  //   the production entry point routes through EventStore.open so the
  //   lib-version check fires in real app boots, not only in direct
  //   EventStore.open calls.
  // -------------------------------------------------------------------------
  group('bootstrapEventStore routes through EventStore.open', () {
    Future<LocalLibVersionHistory> history(StorageBackend backend) =>
        backend.transaction((txn) => VersionCheck.readLocalInTxn(backend, txn));

    // Verifies: EVS-DEV-event-store-open/B
    test('emits lib_version_initialized on first bootstrap', () async {
      final backend = await _openBackend();
      final bundle = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[],
        destinations: const <Destination>[],
      );
      final recorded = (await history(backend)).latest;
      expect(recorded, isNotNull);
      expect(recorded!.packageVersion, LibVersion.version);
      expect(recorded.dataFormat, LibVersion.dataFormat);
      expect(recorded.databaseId, bundle.eventStore.databaseId);
      expect(recorded.event.eventType, LibVersionEvents.initialized);
    });

    // Verifies: EVS-DEV-event-store-open/C
    test('an older build of the same data-format major opens through '
        'bootstrap and records the change', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '99.0.0',
        dataFormat: LibVersion.dataFormat.nextMinor,
      );
      final bundle = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[],
        destinations: const <Destination>[],
      );
      expect(bundle.eventStore, isA<EventStore>());
      final recorded = (await history(backend)).events;
      expect(recorded, hasLength(2));
      expect(recorded.last.event.eventType, LibVersionEvents.changed);
      expect(recorded.last.event.data['fromVersion'], '99.0.0');
      expect(recorded.last.packageVersion, LibVersion.version);
      expect(recorded.last.dataFormat, LibVersion.dataFormat);
    });

    // Verifies: EVS-DEV-event-store-open/D
    test('a database of another data-format major is refused through '
        'bootstrap before any write', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '99.0.0',
        dataFormat: DataFormatVersion(LibVersion.dataFormat.major + 1, 0),
      );
      final counter = await backend.readSequenceCounter();
      await expectLater(
        bootstrapEventStore(
          backend: backend,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[],
          destinations: const <Destination>[],
        ),
        throwsA(isA<DataFormatIncompatibleError>()),
      );
      expect(await backend.readSequenceCounter(), counter);
    });
  });
}
