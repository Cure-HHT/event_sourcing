// Backend-agnostic scenarios for the causal record the library stamps on
// every append and carries unchanged on ingest: kind and eligibility come
// from the entry type's declaration for the event type (reserved event
// types being ineligible annotations), and parents name the aggregate's
// latest eligible version in the appending database's log, read inside the
// append transaction, ingested events counted by their recorded causal. Run on Sembast by
// test/event_store/causal_stamping_test.dart and on Postgres by
// test/storage/postgres/postgres_causal_stamping_test.dart.
//
// Traceability lives on the individual tests below.
import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kIngestDuplicateReceivedEventType, kSecurityContextRedactedEntryType;
import 'package:flutter_test/flutter_test.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

const _kType = 'causal_note';

/// An eligible version: an event type the definition does not declare.
const _kVersion = 'revised';

/// An annotation declared eligible, so only its kind keeps it from being
/// named as a parent.
const _kAnnotation = 'scored';

/// A version declared ineligible, such as a draft.
const _kDraft = 'drafted';

const _kInitiator = UserInitiator('causal-user');

const List<EventTypeDeclaration> _kDeclarations = <EventTypeDeclaration>[
  EventTypeDeclaration(
    eventType: _kAnnotation,
    kind: CausalKind.annotation,
    eligible: true,
  ),
  EventTypeDeclaration(
    eventType: _kDraft,
    kind: CausalKind.version,
    eligible: false,
  ),
];

/// A receiver's declaration of [_kVersion] that differs from the sender's:
/// an ineligible annotation.
const List<EventTypeDeclaration> _kOtherDeclarations = <EventTypeDeclaration>[
  EventTypeDeclaration(
    eventType: _kVersion,
    kind: CausalKind.annotation,
    eligible: false,
  ),
];

EntryTypeRegistry _registry(List<EventTypeDeclaration> declarations) =>
    EntryTypeRegistry()..register(
      EntryTypeDefinition(
        id: _kType,
        registeredVersion: const EntryTypeVersion(1, 0),
        name: _kType,
        declarations: declarations,
      ),
    );

Future<StoredEvent> _append(
  EventStore store,
  String aggregateId,
  String eventType, {
  SecurityDetails? security,
}) async => (await store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: _kType,
  eventType: eventType,
  data: <String, Object?>{'n': DateTime.now().microsecondsSinceEpoch},
  initiator: _kInitiator,
  security: security,
))!;

CausalRef _ref(StoredEvent event) =>
    CausalRef(eventId: event.eventId, eventHash: event.sealedHash);

/// The events of [store]'s log, oldest first.
Future<List<StoredEvent>> _log(EventStore store) =>
    store.reader.findAllEvents();

/// The only event of [store]'s log matching [test].
Future<StoredEvent> _single(
  EventStore store,
  bool Function(StoredEvent) test,
) async {
  final matches = (await _log(store)).where(test).toList();
  expect(matches, hasLength(1));
  return matches.single;
}

/// Expects [event] to carry the causal record of a version or annotation
/// of the given [kind] and [eligible] that follows [parents].
void _expectCausal(
  StoredEvent event, {
  required CausalKind kind,
  required bool eligible,
  required List<CausalRef> parents,
}) {
  final causal = event.causal;
  expect(causal, isNotNull, reason: '${event.eventType} carries no causal');
  expect(causal!.kind, kind, reason: event.eventType);
  expect(causal.eligible, eligible, reason: event.eventType);
  expect(causal.parents, parents, reason: event.eventType);
  expect(
    causal.toJson().keys,
    unorderedEquals(<String>['kind', 'eligible', 'parents']),
    reason: event.eventType,
  );
}

/// Runs the causal-stamping scenarios. [openDatabase] and
/// [openOtherDatabase] return two distinct databases; [skip] skips the
/// group when set.
void runCausalStampingScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required Future<VersionTestDatabase?> Function() openOtherDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('causal stamping ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open({
      bool other = false,
      String identifier = 'causal-install',
      List<EventTypeDeclaration> declarations = _kDeclarations,
    }) async {
      final db = (await (other ? openOtherDatabase() : openDatabase()))!;
      databases.add(db);
      final backend = await db.openBackend();
      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
        entryTypes: _registry(declarations),
        source: Source(
          hopId: 'causal-hop',
          identifier: identifier,
          softwareVersion: 'causal-app@1.0.0',
        ),
      );
      opened.add(store);
      return store;
    }

    tearDown(() async {
      for (final s in opened.reversed) {
        await s.close();
      }
      opened.clear();
      for (final d in databases.reversed) {
        await d.close();
      }
      databases.clear();
    });

    // Verifies: EVS-DEV-causal-parents/F
    // Verifies: EVS-DEV-causal-parents/H
    test('the first version of an aggregate follows no parent, and an '
        'annotation with no version held is a root annotation', () async {
      final store = await open();
      final root = await _append(store, 'note-root', _kAnnotation);
      final first = await _append(store, 'note-1', _kVersion);

      _expectCausal(
        root,
        kind: CausalKind.annotation,
        eligible: true,
        parents: const <CausalRef>[],
      );
      _expectCausal(
        first,
        kind: CausalKind.version,
        eligible: true,
        parents: const <CausalRef>[],
      );
      final stored = await _single(store, (e) => e.eventId == first.eventId);
      expect(stored.causal, first.causal);
    });

    // Verifies: EVS-DEV-causal-parents/F
    // Verifies: EVS-DEV-causal-parents/H
    test('an annotation between two versions follows the first, and the '
        'second version follows the first, not the annotation', () async {
      final store = await open();
      final v1 = await _append(store, 'note-1', _kVersion);
      final a = await _append(store, 'note-1', _kAnnotation);
      final v2 = await _append(store, 'note-1', _kVersion);

      _expectCausal(
        a,
        kind: CausalKind.annotation,
        eligible: true,
        parents: <CausalRef>[_ref(v1)],
      );
      _expectCausal(
        v2,
        kind: CausalKind.version,
        eligible: true,
        parents: <CausalRef>[_ref(v1)],
      );
      for (final event in <StoredEvent>[v1, a, v2]) {
        final stored = await _single(store, (e) => e.eventId == event.eventId);
        expect(stored.causal, event.causal, reason: event.eventType);
        expect(
          stored.toMap()['causal'],
          event.toMap()['causal'],
          reason: event.eventType,
        );
      }
    });

    // Verifies: EVS-DEV-causal-parents/F
    // Verifies: EVS-DEV-causal-parents/H
    test('an ineligible version is stamped with its parents but is never '
        'named as one, and another aggregate never is', () async {
      final store = await open();
      final v1 = await _append(store, 'note-1', _kVersion);
      final draft = await _append(store, 'note-1', _kDraft);
      await _append(store, 'note-2', _kVersion);
      final v2 = await _append(store, 'note-1', _kVersion);

      _expectCausal(
        draft,
        kind: CausalKind.version,
        eligible: false,
        parents: <CausalRef>[_ref(v1)],
      );
      _expectCausal(
        v2,
        kind: CausalKind.version,
        eligible: true,
        parents: <CausalRef>[_ref(v1)],
      );
    });

    // Verifies: EVS-DEV-causal-parents/F
    test('the events the library appends, through its reserved appends and '
        'its raw internal appends, are ineligible annotations', () async {
      final store = await open();
      final subject = await _append(
        store,
        'note-1',
        _kVersion,
        security: const SecurityDetails(ipAddress: '10.0.0.1'),
      );
      await store.clearSecurityContext(
        subject.eventId,
        reason: 'causal redaction',
        redactedBy: _kInitiator,
      );
      final peer = await open(other: true, identifier: 'causal-peer');
      final sent = await _append(peer, 'peer-note', _kVersion);
      await store.ingestEvent(sent);
      await store.ingestEvent(sent);

      final reserved = <StoredEvent>[
        await _single(
          store,
          (e) => e.eventType == LibVersionEvents.initialized,
        ),
        await _single(
          store,
          (e) => e.entryType == kSecurityContextRedactedEntryType,
        ),
        await _single(
          store,
          (e) => e.eventType == kIngestDuplicateReceivedEventType,
        ),
      ];
      for (final event in reserved) {
        _expectCausal(
          event,
          kind: CausalKind.annotation,
          eligible: false,
          parents: const <CausalRef>[],
        );
      }
    });

    // Verifies: EVS-DEV-causal-parents/H
    test('an ingested version is the latest eligible version by its '
        'recorded causal, named by its sealed hash', () async {
      final store = await open();
      final peer = await open(other: true, identifier: 'causal-peer');
      final local = await _append(store, 'shared-note', _kVersion);
      final sent = await _append(peer, 'shared-note', _kVersion);
      await store.ingestEvent(sent);
      final ingested = await _single(store, (e) => e.eventId == sent.eventId);
      expect(ingested.eventHash, isNot(sent.eventHash));

      final next = await _append(store, 'shared-note', _kVersion);

      expect(local.causal!.parents, isEmpty);
      _expectCausal(
        next,
        kind: CausalKind.version,
        eligible: true,
        parents: <CausalRef>[
          CausalRef(eventId: sent.eventId, eventHash: sent.eventHash),
        ],
      );
    });

    // Verifies: EVS-DEV-event-record/J
    test('ingest keeps the incoming causal object unchanged when the '
        'receiver declares its event type otherwise', () async {
      final store = await open(declarations: _kOtherDeclarations);
      final peer = await open(other: true, identifier: 'causal-peer');
      final v1 = await _append(peer, 'shared-note', _kVersion);
      final sent = await _append(peer, 'shared-note', _kVersion);
      expect(sent.causal!.parents, <CausalRef>[_ref(v1)]);

      await store.ingestEvent(v1);
      await store.ingestEvent(sent);

      final stored = await _single(store, (e) => e.eventId == sent.eventId);
      expect(stored.causal, sent.causal);
      expect(
        canonicalize(stored.toMap()['causal']),
        canonicalize(sent.toMap()['causal']),
      );

      // The receiver's own append of that event type is stamped from its
      // own declaration.
      final own = await _append(store, 'shared-note', _kVersion);
      _expectCausal(
        own,
        kind: CausalKind.annotation,
        eligible: false,
        parents: <CausalRef>[
          CausalRef(eventId: sent.eventId, eventHash: sent.eventHash),
        ],
      );
    });
  });
}
