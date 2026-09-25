// Run-time storage barrier scenarios, shared by the Sembast and Postgres
// runners. Every object the library hands to application code over storage
// the library opened -- the event store, its storage reader, its
// security-context store, its idempotency store, the bootstrap bundle, the
// destination registry, a started delivery cycle, a transaction handle and
// publish collector inside a body, a storage-reader transaction handle, the
// authorization policy, the action dispatcher, a boot-progress report and a
// subscription emission -- refuses, when invoked dynamically, every member
// name that would write, append a reserved event, publish or yield a
// storage handle, and cannot be downcast to a writing type. The public
// operations keep working.
//
// The scenarios' assertions are cited on the tests of the runners
// (run_time_barrier_test.dart, postgres_run_time_barrier_test.dart), where
// the annotations bind; this helper declares no test file of its own.
// The scenarios invoke members dynamically and catch the NoSuchMethodError
// and TypeError those invocations must raise: that is what they check.
// ignore_for_file: avoid_dynamic_calls, avoid_catching_errors
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart'
    show MutableSecurityContextStore;
import 'package:flutter_test/flutter_test.dart';

import '../test_support/fake_destination.dart';

const Source barrierSource = Source(
  hopId: 'barrier-test',
  identifier: 'aaaa0001-0000-4000-8000-0000000ba221',
  softwareVersion: 'barrier-test@1',
);

const Source _peerSource = Source(
  hopId: 'barrier-peer',
  identifier: 'aaaa0001-0000-4000-8000-0000000ba222',
  softwareVersion: 'barrier-test@1',
);

const EntryTypeDefinition barrierNoteType = EntryTypeDefinition(
  id: 'barrier_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Barrier note',
);

const String _notesView = 'barrier_notes';

/// The member names no handed-out object may carry: each would write the
/// library's persisted state outside a public operation, append a reserved
/// event, append without the event store's checks, publish to live
/// subscribers, set or fire the delivery trigger, or yield a backend, a
/// database, a pool, a session or an engine transaction.
const List<String> forbiddenMemberNames = <String>[
  'backend',
  'appendReserved',
  'appendReservedInTxn',
  'deliveryTrigger',
  'wakeDeliveryCycle',
  'add',
  'addRowChanges',
  'session',
  'txn',
  'database',
  'db',
  'pool',
  'setViewTargetVersion',
  'writeInTxn',
  'owner',
  'invalidate',
  'logRejectedBatch',
];

/// The further names a transaction handle may not carry: the engine
/// handles and the handle's own bookkeeping.
const List<String> forbiddenHandleNames = <String>[
  'sembastTxn',
  'connection',
  'wroteBackendState',
];

/// Reads [name] from [o] through a dynamic call: a field, a getter, or a
/// method tear-off. Throws `NoSuchMethodError` when [o] carries no member
/// of that name reachable from this library.
Object? dynamicRead(Object o, String name) {
  final dynamic d = o;
  return switch (name) {
    'backend' => d.backend,
    'appendReserved' => d.appendReserved,
    'appendReservedInTxn' => d.appendReservedInTxn,
    'deliveryTrigger' => d.deliveryTrigger,
    'wakeDeliveryCycle' => d.wakeDeliveryCycle,
    'add' => d.add,
    'addRowChanges' => d.addRowChanges,
    'session' => d.session,
    'txn' => d.txn,
    'database' => d.database,
    'db' => d.db,
    'pool' => d.pool,
    'setViewTargetVersion' => d.setViewTargetVersion,
    'writeInTxn' => d.writeInTxn,
    'owner' => d.owner,
    'invalidate' => d.invalidate,
    'logRejectedBatch' => d.logRejectedBatch,
    'sembastTxn' => d.sembastTxn,
    'connection' => d.connection,
    'wroteBackendState' => d.wroteBackendState,
    _ => throw ArgumentError.value(name, 'name', 'not a listed name'),
  };
}

/// The names in [names] that [o] answers when invoked dynamically, each
/// with what it answered; every other name throws `NoSuchMethodError`.
List<String> forbiddenMembersAnswered(
  Object o,
  String label, {
  List<String> names = forbiddenMemberNames,
}) {
  final answered = <String>[];
  for (final name in names) {
    try {
      final value = dynamicRead(o, name);
      answered.add('$name -> ${value.runtimeType}');
    } on NoSuchMethodError {
      // Refused, as required.
    }
  }
  try {
    (o as dynamic).deliveryTrigger = null;
    answered.add('deliveryTrigger= accepted');
  } on NoSuchMethodError {
    // Refused, as required.
  }
  return <String>[for (final a in answered) '$label (${o.runtimeType}): $a'];
}

/// The writing types the library declares that [o] downcasts to; a
/// downcast to every other one throws `TypeError`.
List<String> writingDowncastsAccepted(Object o, String label) {
  final accepted = <String>[];
  final casts = <String, void Function()>{
    'StorageBackend': () => o as StorageBackend,
    'SembastBackend': () => o as SembastBackend,
    'PostgresBackend': () => o as PostgresBackend,
    'MutableSecurityContextStore': () => o as MutableSecurityContextStore,
    'SembastSecurityContextStore': () => o as SembastSecurityContextStore,
    'PostgresSecurityContextStore': () => o as PostgresSecurityContextStore,
  };
  for (final MapEntry(key: type, value: cast) in casts.entries) {
    try {
      cast();
      accepted.add('$label (${o.runtimeType}) downcasts to $type');
    } on TypeError {
      // Refused, as required.
    }
  }
  return accepted;
}

/// The ways a transaction handle is not opaque: a run-time type that is
/// not private to the library declaring it (application code could name it
/// in a downcast), or a member that answers a forbidden name.
List<String> handleOpenings(Transaction txn, String label) => <String>[
  if (!txn.runtimeType.toString().startsWith('_'))
    '$label: its type ${txn.runtimeType} is public, so a downcast reaches it',
  ...forbiddenMembersAnswered(
    txn,
    label,
    names: <String>[...forbiddenMemberNames, ...forbiddenHandleNames],
  ),
];

class _NotesProjections {
  static ProjectionRegistry build() => ProjectionRegistry()
    ..register(
      const AggregateProjectionSpec(
        viewName: _notesView,
        interest: SubscriptionFilter(entryTypes: <String>{'barrier_note'}),
        tombstoneEventTypes: <String>{},
      ),
    );
}

class _Opened {
  _Opened(this.bundle, this.progress);

  final EventStoreBundle bundle;
  final BootProgress progress;

  EventStore get store => bundle.eventStore;
}

Future<_Opened> _bootstrap(StorageDescription storage) async {
  BootProgress? progress;
  final bundle = await bootstrapEventStore(
    storage: storage,
    source: barrierSource,
    entryTypes: const <EntryTypeDefinition>[barrierNoteType],
    destinations: <Destination>[
      FakeDestination(
        id: 'barrier-dest',
        filter: const SubscriptionFilter(entryTypes: <String>{'barrier_note'}),
      ),
    ],
    projections: _NotesProjections.build(),
    onBootProgress: (p) => progress = p,
  );
  return _Opened(bundle, progress!);
}

Future<StoredEvent?> _appendNote(EventStore store, String id) => store.append(
  entryType: barrierNoteType.id,
  aggregateId: id,
  aggregateType: 'BarrierNote',
  eventType: 'written',
  data: <String, Object?>{'text': 'note $id'},
  initiator: const UserInitiator('barrier-user'),
  security: const SecurityDetails(ipAddress: '10.0.0.9'),
);

/// Runs the barrier scenarios over storage the library opens from the
/// description [freshStorage] returns (a new, empty database each call).
/// [expectsIdempotencyStore] is true where the event store builds an
/// idempotency store over its storage. [deleteStorage], when given, deletes
/// a closed database by its description.
void runRunTimeBarrierScenarios({
  required Future<StorageDescription> Function() freshStorage,
  required String backendLabel,
  required bool expectsIdempotencyStore,
  Future<void> Function(StorageDescription storage)? deleteStorage,
  String? skip,
}) {
  group('run-time barrier on $backendLabel', skip: skip, () {
    test('no handed-out object answers a forbidden member dynamically, and '
        'none downcasts to a writing type', () async {
      final opened = await _bootstrap(await freshStorage());
      final store = opened.store;
      SyncCycle? cycle;
      StreamSubscription<Update<Object?>>? sub;
      try {
        final emissions = <Update<Object?>>[];
        sub = store
            .subscribe<Object?>(const SubscriptionFilter(), const Events())
            .listen(emissions.add);
        await _appendNote(store, 'n-1');
        for (var i = 0; i < 100 && emissions.isEmpty; i++) {
          await Future<void>.delayed(const Duration(milliseconds: 10));
        }
        expect(emissions, isNotEmpty, reason: 'no subscription emission');

        cycle = await SyncCycle.start(
          registry: opened.bundle.destinations,
          cadence: const Duration(hours: 1),
        );
        final policy = TableBackedAuthorizationPolicy(
          reader: store.reader,
          scopeClassRegistry: ScopeClassRegistry(
            classes: const <ScopeClassSpec>[],
            projectionLookup: (_) => null,
          ),
        );
        final dispatcher = ActionDispatcher(
          registry: ActionRegistry(),
          authorization: policy,
          events: store,
          idempotency: store.idempotencyStore ?? InMemoryIdempotencyStore(),
        );
        if (expectsIdempotencyStore) {
          expect(store.idempotencyStore, isNotNull);
        }

        final handedOut = <String, Object>{
          'event store': store,
          'storage reader': store.reader,
          'security-context store': store.securityContexts,
          'bootstrap bundle': opened.bundle,
          'bundle security-context store': opened.bundle.securityContexts,
          'destination registry': opened.bundle.destinations,
          'delivery cycle': cycle,
          'authorization policy': policy,
          'action dispatcher': dispatcher,
          'boot progress': opened.progress,
          'subscription emission': emissions.first,
          'entry-type registry': store.entryTypes,
          'projection registry': store.projections,
          'promoter registry': store.promoters,
          if (store.idempotencyStore != null)
            'idempotency store': store.idempotencyStore!,
        };
        final violations = <String>[];
        handedOut.forEach((label, o) {
          violations
            ..addAll(forbiddenMembersAnswered(o, label))
            ..addAll(writingDowncastsAccepted(o, label));
        });

        await store.runTransaction((txn, collector) async {
          violations
            ..addAll(handleOpenings(txn, 'runTransaction handle'))
            ..addAll(writingDowncastsAccepted(txn, 'runTransaction handle'))
            ..addAll(forbiddenMembersAnswered(collector, 'publish collector'))
            ..addAll(writingDowncastsAccepted(collector, 'publish collector'));
        });
        await store.reader.transaction((txn) async {
          violations
            ..addAll(handleOpenings(txn, 'storage-reader handle'))
            ..addAll(writingDowncastsAccepted(txn, 'storage-reader handle'));
        });
        expect(
          violations,
          isEmpty,
          reason:
              'handed-out objects answered dynamic calls or downcasts they '
              'must refuse (NoSuchMethodError, TypeError)',
        );
      } finally {
        await sub?.cancel();
        await cycle?.close();
        await store.close();
      }
    });

    test('the public operations still work through the handed-out '
        'objects', () async {
      final storage = await freshStorage();
      final opened = await _bootstrap(storage);
      final store = opened.store;
      final peerStorage = await freshStorage();
      final peer = await EventStore.open(
        storage: peerStorage,
        entryTypes: EntryTypeRegistry()..register(barrierNoteType),
        source: _peerSource,
      );
      try {
        // append and runTransaction.
        final first = await _appendNote(store, 'n-1');
        expect(first, isNotNull);
        final inTxn = await store.runTransaction(
          (txn, collector) => store.appendInTxn(
            txn,
            collector: collector,
            entryType: barrierNoteType.id,
            aggregateId: 'n-2',
            aggregateType: 'BarrierNote',
            eventType: 'written',
            data: const <String, Object?>{'text': 'in txn'},
            initiator: const UserInitiator('barrier-user'),
            flowToken: null,
            metadata: null,
            security: null,
            checkpointReason: null,
            changeReason: null,
            dedupeByContent: false,
          ),
        );
        expect(inTxn, isNotNull);

        // ingest of a peer's event.
        final peerEvent = await _appendNote(peer, 'p-1');
        final outcome = await store.ingestEvent(peerEvent!);
        expect(outcome.outcome, IngestOutcome.ingested);

        // registry operations.
        final registry = opened.bundle.destinations;
        final schedule = await registry.scheduleOf('barrier-dest');
        expect(schedule, isNotNull);
        await registry.setStartDate(
          'barrier-dest',
          DateTime.now().toUtc(),
          initiator: const UserInitiator('barrier-user'),
        );

        // delivery cycle start and close.
        final cycle = await SyncCycle.start(
          registry: registry,
          cadence: const Duration(hours: 1),
        );
        await cycle.close();

        // view rebuild.
        final processed = await rebuildView(
          store: store,
          viewName: _notesView,
          targetVersionByEntryType: <String, EntryTypeVersion>{
            barrierNoteType.id: barrierNoteType.registeredVersion,
          },
        );
        expect(processed, greaterThanOrEqualTo(3));
        final rows = await store.reader.findViewRows(_notesView);
        expect(rows, hasLength(3));

        // redaction and retention.
        await store.clearSecurityContext(
          first!.eventId,
          reason: 'barrier test',
          redactedBy: const UserInitiator('barrier-admin'),
        );
        expect(await store.securityContexts.read(first.eventId), isNull);
        final retention = await store.applyRetentionPolicy();
        expect(retention.compactedCount, 0);
      } finally {
        await store.close();
        await peer.close();
      }
      if (deleteStorage != null) {
        await deleteStorage(storage);
        await deleteStorage(peerStorage);
      }
    });
  });
}
