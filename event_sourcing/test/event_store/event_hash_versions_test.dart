// An event's entry-type version and data-format version are part of the
// content its hash is derived from: rewriting either on a stored or
// forwarded copy breaks the copy's hash and, at the next hop, the chain.
import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/verification/chain_walk.dart'
    show hashMismatchEvidence;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _kType = 'hashed_note';
var _dbCounter = 0;

Future<EventStore> _openStore(String hop) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'event-hash-$hop-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: _kType,
      registeredVersion: EntryTypeVersion(1, 2),
      name: _kType,
    ),
  );
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: Source(
      hopId: hop,
      identifier: '$hop-install',
      softwareVersion: 't',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

String _recomputedHash(StoredEvent event) {
  final map = Map<String, Object?>.from(event.toMap())..remove('event_hash');
  return canonicalEventHash(map);
}

StoredEvent _withVersions(
  StoredEvent event, {
  EntryTypeVersion? entryTypeVersion,
  DataFormatVersion? dataFormat,
}) {
  final map = Map<String, Object?>.from(event.toMap());
  if (entryTypeVersion != null) {
    map['entry_type_version'] = entryTypeVersion.toJson();
  }
  if (dataFormat != null) {
    map['lib_format_version'] = dataFormat.toJson();
  }
  return StoredEvent.fromMap(map, event.key);
}

Future<StoredEvent> _appendAt(EventStore store) async {
  final event = await store.append(
    entryType: _kType,
    aggregateId: 'agg-1',
    aggregateType: 'note',
    eventType: 'finalized',
    data: const <String, Object?>{'title': 'hashed'},
    initiator: const UserInitiator('hash-user'),
  );
  return event!;
}

void main() {
  group('event hash covers the versions', () {
    // Verifies: EVS-DEV-version-compatibility/J
    // Verifies: EVS-PRD-hash-chain-integrity/A
    test("an appended event's hash reproduces from its stored content, "
        'versions included', () async {
      final origin = await _openStore('origin');
      final event = await _appendAt(origin);
      expect(_recomputedHash(event), event.eventHash);
      final readBack = await origin.reader.findEventById(event.eventId);
      expect(_recomputedHash(readBack!), event.eventHash);
    });

    // Verifies: EVS-DEV-version-compatibility/J
    // Verifies: EVS-PRD-hash-chain-integrity/A
    test("rewriting a stored event's entry-type or data-format version "
        'breaks its hash', () async {
      final origin = await _openStore('origin');
      final event = await _appendAt(origin);
      final rewrittenEntryType = _withVersions(
        event,
        entryTypeVersion: const EntryTypeVersion(1, 3),
      );
      final rewrittenFormat = _withVersions(
        event,
        dataFormat: LibVersion.dataFormat.nextMinor,
      );
      expect(_recomputedHash(rewrittenEntryType), isNot(event.eventHash));
      expect(_recomputedHash(rewrittenFormat), isNot(event.eventHash));
    });

    // Verifies: EVS-DEV-version-compatibility/J
    // Verifies: EVS-PRD-hash-chain-integrity/A
    // Verifies: EVS-DEV-chain-verification/P
    test('a forwarder that rewrites a version breaks the chain at the next '
        'hop, and the next hop stores the event with a hash_mismatch '
        'finding', () async {
      final origin = await _openStore('origin');
      final relay = await _openStore('relay');
      final receiver = await _openStore('receiver');
      final event = await _appendAt(origin);
      await relay.ingestEvent(event);
      final forwarded = (await relay.reader.findEventById(event.eventId))!;

      final tampered = <String, StoredEvent>{
        'entry type version': _withVersions(
          forwarded,
          entryTypeVersion: const EntryTypeVersion(1, 1),
        ),
        'data-format version': _withVersions(
          forwarded,
          dataFormat: DataFormatVersion(LibVersion.dataFormat.major, 3),
        ),
      };
      for (final entry in tampered.entries) {
        final mismatches = hashMismatchEvidence(entry.value);
        expect(mismatches, isNotEmpty, reason: entry.key);
        final next = await _openStore(
          'receiver-${entry.key.replaceAll(' ', '-')}',
        );
        final outcome = await next.ingestEvent(entry.value);
        expect(
          outcome.outcome,
          IngestOutcome.ingestedWithFinding,
          reason: entry.key,
        );
        final findings = await next.reader.findAllEvents(
          entryType: kSecurityFindingEntryType,
        );
        expect(
          findings.map((f) => f.data['kind']),
          <String>[for (final _ in mismatches) 'hash_mismatch'],
          reason: '${entry.key}: one finding per hash that does not recompute',
        );
      }

      // The untampered copy verifies and ingests.
      expect(hashMismatchEvidence(forwarded), isEmpty);
      await receiver.ingestEvent(forwarded);
      final stored = await receiver.reader.findEventById(event.eventId);
      expect(stored!.entryTypeVersion, const EntryTypeVersion(1, 2));
      expect(_recomputedHash(stored), stored.eventHash);
    });
  });

  group('the event hash', () {
    // Verifies: EVS-DEV-version-compatibility/J
    // Verifies: EVS-PRD-hash-chain-integrity/A
    // Verifies: EVS-PRD-hash-chain-integrity/D
    // Verifies: EVS-DEV-event-record/K
    test('is the SHA-256 of the JCS form of the hashed fields, pinned by a '
        'fixed vector', () {
      final record = <String, Object?>{
        'event_id': 'golden-1',
        'aggregate_id': 'agg-g',
        'aggregate_type': 'note',
        'entry_type': 'golden_note',
        'entry_type_version': const EntryTypeVersion(1, 2).toJson(),
        'lib_format_version': const DataFormatVersion(2, 0).toJson(),
        'event_type': 'finalized',
        'sequence_number': 7,
        'data': <String, Object?>{'title': 'g'},
        'initiator': <String, Object?>{'type': 'user', 'user_id': 'u'},
        'flow_token': null,
        'client_timestamp': '2026-09-01T12:00:00.000Z',
        'previous_event_hash': null,
        'causal': <String, Object?>{
          'kind': 'version',
          'eligible': true,
          'parents': <Object?>[
            <String, Object?>{'event_id': 'p-1', 'event_hash': 'h-1'},
          ],
        },
        'metadata': <String, Object?>{'provenance': <Object?>[]},
        'event_hash': 'ignored',
      };
      // The JCS form: every hashed field, keys sorted; aggregate_type and
      // event_hash are not hashed.
      const canonical =
          '{"aggregate_id":"agg-g",'
          '"causal":{"eligible":true,"kind":"version",'
          '"parents":[{"event_hash":"h-1","event_id":"p-1"}]},'
          '"client_timestamp":"2026-09-01T12:00:00.000Z",'
          '"data":{"title":"g"},'
          '"entry_type":"golden_note",'
          '"entry_type_version":{"major":1,"minor":2},'
          '"event_id":"golden-1",'
          '"event_type":"finalized",'
          '"flow_token":null,'
          '"initiator":{"type":"user","user_id":"u"},'
          '"lib_format_version":{"major":2,"minor":0},'
          '"metadata":{"provenance":[]},'
          '"previous_event_hash":null,'
          '"sequence_number":7}';
      expect(
        sha256.convert(utf8.encode(canonical)).toString(),
        '7e2711ec92786e45f8c4e3651c6d6e5de215a4ab92834537f0af1d32ef0b2ecc',
      );
      expect(
        canonicalEventHash(record),
        '7e2711ec92786e45f8c4e3651c6d6e5de215a4ab92834537f0af1d32ef0b2ecc',
      );
    });
  });
}
