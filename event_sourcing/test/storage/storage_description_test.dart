// Verifies: EVS-PRD-storage-barrier/A
// Verifies: EVS-PRD-storage-barrier/G
// Verifies: EVS-PRD-storage-barrier/H
// Verifies: EVS-PRD-storage-barrier/I
// Verifies: EVS-DEV-storage-capability/A
// Verifies: EVS-DEV-storage-capability/B
// Verifies: EVS-DEV-storage-capability/E
// Verifies: EVS-DEV-storage-capability/J
// Verifies: EVS-DEV-storage-capability/L
//
// Storage descriptions on the native runtime: the library opens the
// storage a Sembast description names with a factory it selects, closes it
// when the event store closes and when the open fails after it opened it,
// and deletes it by location only while no event store of this isolate
// holds it open. A backend the application supplies is accepted only named
// as such and stays open after the store closes. The security-context
// store the store and the bundle hand out declares no writing member.
@TestOn('vm')
library;

import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart'
    show MutableSecurityContextStore;
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;
import 'package:sembast/sembast_memory.dart';

const Source _source = Source(
  hopId: 'description-test',
  identifier: 'description-install',
  softwareVersion: 'description-test@1',
);

const _noteType = EntryTypeDefinition(
  id: 'description_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Description note',
);

var _counter = 0;

/// A memory description under a name no other test of this isolate uses.
SembastStorage _uniqueMemory() => SembastStorage.memory(
  'storage-description-${DateTime.now().microsecondsSinceEpoch}-'
  '${_counter++}.db',
);

EntryTypeRegistry _registry() => EntryTypeRegistry()..register(_noteType);

Future<EventStore> _open(StorageDescription storage) =>
    EventStore.open(storage: storage, entryTypes: _registry(), source: _source);

Future<StoredEvent?> _appendNote(EventStore store, {String id = 'n-1'}) =>
    store.append(
      entryType: _noteType.id,
      aggregateId: id,
      aggregateType: 'DescriptionNote',
      eventType: 'written',
      data: const <String, Object?>{'text': 'hello'},
      initiator: const UserInitiator('description-user'),
      security: const SecurityDetails(ipAddress: '10.0.0.1'),
    );

Future<List<StoredEvent>> _notes(EventStore store) =>
    store.reader.findAllEvents(entryType: _noteType.id);

void main() {
  group('a Sembast memory description', () {
    test('opens and appends; close releases it, so the delete succeeds and '
        'a reopen is empty', () async {
      final storage = _uniqueMemory();
      final store = await _open(storage);
      await _appendNote(store);
      expect(await _notes(store), hasLength(1));
      await store.close();

      await deleteSembastDatabase(storage);

      final reopened = await _open(storage);
      addTearDown(() async {
        await reopened.close();
        await deleteSembastDatabase(storage);
      });
      expect(await _notes(reopened), isEmpty);
    });

    test('while a store holds it open, the delete refuses with StateError '
        'and the data survives', () async {
      final storage = _uniqueMemory();
      final store = await _open(storage);
      addTearDown(() async {
        await store.close();
        await deleteSembastDatabase(storage);
      });
      await _appendNote(store);

      await expectLater(deleteSembastDatabase(storage), throwsStateError);

      expect(await _notes(store), hasLength(1));
      await _appendNote(store, id: 'n-2');
      expect(await _notes(store), hasLength(2));
    });

    test('a boot refusal closes the storage it opened before the error '
        'reaches the caller', () async {
      final storage = _uniqueMemory();
      final first = await _open(storage);
      await first.close();
      // Tamper with the stored database identity outside the library.
      final db = await databaseFactoryMemory.openDatabase(storage.location);
      await StoreRef<String, Object?>(
        'backend_state',
      ).record('database_id').put(db, 'tampered-identity');
      await db.close();

      await expectLater(
        _open(storage),
        throwsA(isA<DatabaseIdentityMismatchError>()),
      );

      // The failed open released the location: the delete is not refused.
      await deleteSembastDatabase(storage);
    });

    test('a failure inside bootstrapEventStore after the open closes the '
        'storage before the error reaches the caller', () async {
      final storage = _uniqueMemory();
      final duplicate = _NoopDestination('dup');
      await expectLater(
        bootstrapEventStore(
          storage: storage,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[_noteType],
          destinations: <Destination>[duplicate, _NoopDestination('dup')],
        ),
        throwsA(anything),
      );

      await deleteSembastDatabase(storage);
    });
  });

  test('a Sembast file description writes the database file at its path '
      'with the native file factory', () async {
    final dir = await Directory.systemTemp.createTemp('storage_description_');
    addTearDown(() => dir.delete(recursive: true));
    final path = p.join(dir.path, 'events.db');
    final storage = SembastStorage.file(path);

    final store = await _open(storage);
    await _appendNote(store);
    await store.close();

    expect(File(path).existsSync(), isTrue);
    expect(File(path).readAsStringSync(), contains(_noteType.id));

    await deleteSembastDatabase(storage);
    expect(File(path).existsSync(), isFalse);
  });

  test('an application-supplied backend stays open after the store over it '
      'closes', () async {
    final db = await newDatabaseFactoryMemory().openDatabase('supplied.db');
    final backend = SembastBackend(database: db);
    addTearDown(backend.close);
    final store = await _open(
      ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
    );
    await _appendNote(store);
    await store.close();

    final events = await backend.findAllEvents(entryType: _noteType.id);
    expect(events, hasLength(1));
  });

  group('the security-context store handed out', () {
    late SembastStorage storage;
    late EventStoreBundle bundle;

    setUp(() async {
      storage = _uniqueMemory();
      bundle = await bootstrapEventStore(
        storage: storage,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_noteType],
        destinations: const <Destination>[],
      );
    });

    tearDown(() async {
      await bundle.eventStore.close();
      await deleteSembastDatabase(storage);
    });

    test('is a separate read-only object: no writing type, no writing '
        'member at run time', () async {
      final event = (await _appendNote(bundle.eventStore))!;
      for (final contexts in <SecurityContextStore>[
        bundle.securityContexts,
        bundle.eventStore.securityContexts,
      ]) {
        expect(contexts, isNot(isA<MutableSecurityContextStore>()));
        expect(
          () => (contexts as dynamic).writeInTxn(null, null),
          throwsNoSuchMethodError,
        );
        expect(
          () => (contexts as dynamic).deleteInTxn(null, event.eventId),
          throwsNoSuchMethodError,
        );
        expect((await contexts.read(event.eventId))?.ipAddress, '10.0.0.1');
      }
    });

    test('redaction and retention still reach the stored contexts', () async {
      final redacted = (await _appendNote(bundle.eventStore, id: 'r'))!;
      final retained = (await _appendNote(bundle.eventStore, id: 'k'))!;

      await bundle.eventStore.clearSecurityContext(
        redacted.eventId,
        reason: 'test redaction',
        redactedBy: const UserInitiator('description-admin'),
      );
      expect(await bundle.securityContexts.read(redacted.eventId), isNull);

      final result = await bundle.eventStore.applyRetentionPolicy(
        policy: const SecurityRetentionPolicy(
          fullRetention: Duration.zero,
          truncatedRetention: Duration.zero,
        ),
      );
      expect(result.purgedCount, greaterThanOrEqualTo(1));
      expect(await bundle.securityContexts.read(retained.eventId), isNull);
    });
  });
}

/// A destination that is never delivered to; two with one id make the
/// bootstrap's destination registration fail after the open.
class _NoopDestination extends Destination {
  _NoopDestination(this.id);

  @override
  final String id;

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'never'});

  @override
  String get wireFormat => 'noop-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw UnimplementedError();

  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}
