// Verifies: EVS-DEV-destination-drain/L
// the library declares, for every reserved system entry type, the one
//   aggregate type and the event types it appends that entry type with; no
//   two reserved entry types share a declared pair, so the pair identifies
//   the entry type; the declared shapes are fixed within a data-format
//   major; and the library's reserved-append operations refuse a shape it
//   does not declare, and a destination audit whose data ingest would
//   refuse.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kDestinationAuditAggregateType,
        kDestinationAuditEntryTypes,
        kReservedEventShapes;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const Initiator _init = AutomationInitiator(service: 'shapes-test');
const String _noteType = 'shape_note';

/// The declared shape of every reserved entry type -- its aggregate type
/// and its sorted event types -- recorded per data-format major, in literal
/// strings.
///
/// A declared shape is fixed within a data-format major: a build of the
/// same major refuses at ingest a reserved event outside the shapes it
/// declares, so a build that added an event type to a reserved entry type,
/// or changed its aggregate type, would have its events refused by older
/// builds of its own major. An entry recorded under a major is never
/// edited. A new kind of reserved event is a new reserved entry type (a
/// data-format minor step), recorded by adding its entry; a changed shape
/// is recorded under the next data-format major.
const Map<int, Map<String, List<Object>>> _shapesByDataFormatMajor =
    <int, Map<String, List<Object>>>{
      2: <String, List<Object>>{
        'security_context_redacted': <Object>[
          'security_context',
          <String>['security_context_redacted'],
        ],
        'security_context_compacted': <Object>[
          'security_context',
          <String>['security_context_compacted'],
        ],
        'security_context_purged': <Object>[
          'security_context',
          <String>['security_context_purged'],
        ],
        'system.destination_registered': <Object>[
          'system_destination',
          <String>['destination_registered'],
        ],
        'system.destination_start_date_set': <Object>[
          'system_destination',
          <String>['destination_start_date_set'],
        ],
        'system.destination_end_date_set': <Object>[
          'system_destination',
          <String>['destination_end_date_set'],
        ],
        'system.destination_deleted': <Object>[
          'system_destination',
          <String>['destination_deleted'],
        ],
        'system.destination_wedge_recovered': <Object>[
          'system_destination',
          <String>['destination_wedge_recovered'],
        ],
        'system.destination_wedged': <Object>[
          'system_destination',
          <String>['destination_wedged'],
        ],
        'system.retention_policy_applied': <Object>[
          'system_retention',
          <String>['finalized'],
        ],
        'system.entry_type_registry_initialized': <Object>[
          'system_registry',
          <String>['finalized'],
        ],
        'lib_version_initialized': <Object>[
          '_lib',
          <String>['lib_version_initialized'],
        ],
        'lib_version_changed': <Object>[
          '_lib',
          <String>['lib_version_changed'],
        ],
        'ingest-audit': <Object>[
          'ingest-audit',
          <String>['ingest.batch_rejected', 'ingest.duplicate_received'],
        ],
        'view_snapshot_promoted': <Object>[
          '_lib',
          <String>['finalized'],
        ],
      },
    };

Future<EventStore> _open() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'reserved-shapes-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  return EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: _noteType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: _noteType,
        ),
      ),
    source: const Source(
      hopId: 'server',
      identifier: 'shapes-install',
      softwareVersion: 'test@1.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

void main() {
  group('declared shapes of reserved events', () {
    test('every reserved entry type has one declared shape', () {
      expect(kReservedEventShapes.keys.toSet(), kReservedSystemEntryTypeIds);
      expect(<String>{
        for (final d in kSystemEntryTypes) d.id,
      }, kReservedSystemEntryTypeIds);
      for (final entry in kReservedEventShapes.entries) {
        expect(entry.value.aggregateType, isNotEmpty, reason: entry.key);
        expect(entry.value.eventTypes, isNotEmpty, reason: entry.key);
      }
    });

    test('no two reserved entry types share a declared pair', () {
      final owners = <(String, String), String>{};
      for (final entry in kReservedEventShapes.entries) {
        for (final eventType in entry.value.eventTypes) {
          final pair = (entry.value.aggregateType, eventType);
          expect(
            owners[pair],
            isNull,
            reason:
                '$pair is declared for both ${owners[pair]} and '
                '${entry.key}',
          );
          owners[pair] = entry.key;
        }
      }
    });

    test('destination audits use the audit aggregate type and their own '
        'event type constants', () {
      const byKind = <String, String>{
        kDestinationRegisteredEntryType: kDestinationRegisteredEventType,
        kDestinationStartDateSetEntryType: kDestinationStartDateSetEventType,
        kDestinationEndDateSetEntryType: kDestinationEndDateSetEventType,
        kDestinationDeletedEntryType: kDestinationDeletedEventType,
        kDestinationWedgeRecoveredEntryType:
            kDestinationWedgeRecoveredEventType,
        kDestinationWedgedEntryType: kDestinationWedgedEventType,
      };
      expect(kDestinationAuditEntryTypes.toSet(), byKind.keys.toSet());
      expect(kDestinationAuditEntryTypes.toSet(), <String>{
        for (final id in kReservedSystemEntryTypeIds)
          if (id.startsWith('system.destination_')) id,
      });
      for (final entry in byKind.entries) {
        final shape = kReservedEventShapes[entry.key]!;
        expect(shape.aggregateType, kDestinationAuditAggregateType);
        expect(shape.eventTypes, <String>{entry.value});
      }
    });

    test('declared shapes are fixed within a data-format major', () {
      final recorded = _shapesByDataFormatMajor[LibVersion.dataFormat.major];
      expect(
        recorded,
        isNotNull,
        reason:
            'record the declared shapes of data-format major '
            '${LibVersion.dataFormat.major}',
      );
      expect(<String, List<Object>>{
        for (final entry in kReservedEventShapes.entries)
          entry.key: <Object>[
            entry.value.aggregateType,
            entry.value.eventTypes.toList()..sort(),
          ],
      }, recorded);
    });

    test('security-context audits carry their own event types', () {
      expect(
        kReservedEventShapes[kSecurityContextRedactedEntryType]!.eventTypes,
        <String>{kSecurityContextRedactedEventType},
      );
      expect(
        kReservedEventShapes[kSecurityContextCompactedEntryType]!.eventTypes,
        <String>{kSecurityContextCompactedEventType},
      );
      expect(
        kReservedEventShapes[kSecurityContextPurgedEntryType]!.eventTypes,
        <String>{kSecurityContextPurgedEventType},
      );
    });
  });

  group('the library appends reserved events only in a declared shape', () {
    Future<void> expectRefused(
      EventStore store,
      Future<Object?> Function() call,
      String message,
    ) async {
      final before = await store.backend.findAllEvents();
      await expectLater(
        call(),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message.toString(),
            'message',
            contains(message),
          ),
        ),
      );
      expect(
        (await store.backend.findAllEvents()).map((e) => e.eventId),
        before.map((e) => e.eventId),
      );
    }

    test('appendReservedInTxn refuses an undeclared event type', () async {
      final store = await _open();
      await expectRefused(
        store,
        () => store.runTransaction(
          (txn, collector) => store.appendReservedInTxn(
            txn,
            collector,
            entryType: kDestinationWedgedEntryType,
            aggregateId: store.source.identifier,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kDestinationDeletedEventType,
            data: const <String, Object?>{},
            initiator: _init,
          ),
        ),
        'declares',
      );
    });

    test('appendReservedInTxn refuses an undeclared aggregate type', () async {
      final store = await _open();
      await expectRefused(
        store,
        () => store.runTransaction(
          (txn, collector) => store.appendReservedInTxn(
            txn,
            collector,
            entryType: kDestinationWedgedEntryType,
            aggregateId: store.source.identifier,
            aggregateType: 'note',
            eventType: kDestinationWedgedEventType,
            data: const <String, Object?>{},
            initiator: _init,
          ),
        ),
        'declares',
      );
    });

    final badAuditData = <String, Map<String, Object?>>{
      'no database identity': <String, Object?>{'id': 'x'},
      'an empty destination identifier': <String, Object?>{
        'id': '',
        'database_id': 'db',
      },
      'a destination identifier containing |': <String, Object?>{
        'id': 'a|b',
        'database_id': 'db',
      },
      'a database identity containing |': <String, Object?>{
        'id': 'x',
        'database_id': 'db|x',
      },
    };
    for (final c in badAuditData.entries) {
      test('appendReservedInTxn refuses a destination audit with '
          '${c.key}', () async {
        final store = await _open();
        await expectRefused(
          store,
          () => store.runTransaction(
            (txn, collector) => store.appendReservedInTxn(
              txn,
              collector,
              entryType: kDestinationWedgeRecoveredEntryType,
              aggregateId: store.source.identifier,
              aggregateType: kDestinationAuditAggregateType,
              eventType: kDestinationWedgeRecoveredEventType,
              data: c.value,
              initiator: _init,
            ),
          ),
          'destination audit',
        );
      });
    }

    test('appendReserved refuses a destination audit without a database '
        'identity', () async {
      final store = await _open();
      await expectRefused(
        store,
        () => store.appendReserved(
          entryType: kDestinationRegisteredEntryType,
          aggregateId: store.source.identifier,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationRegisteredEventType,
          data: const <String, Object?>{'id': 'x'},
          initiator: _init,
        ),
        'destination audit',
      );
    });

    test('appendReserved refuses an entry type that is not reserved', () async {
      final store = await _open();
      await expectRefused(
        store,
        () => store.appendReserved(
          entryType: _noteType,
          aggregateId: 'n',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{},
          initiator: _init,
        ),
        'not a reserved',
      );
    });

    test(
      'appendReserved appends a declared shape and dedupes by content',
      () async {
        final store = await _open();
        Future<StoredEvent?> audit() => store.appendReserved(
          entryType: kEntryTypeRegistryInitializedEntryType,
          aggregateId: store.source.identifier,
          aggregateType:
              kReservedEventShapes[kEntryTypeRegistryInitializedEntryType]!
                  .aggregateType,
          eventType: 'finalized',
          data: const <String, Object?>{'registry': <String, Object?>{}},
          initiator: _init,
          dedupeByContent: true,
        );
        expect(await audit(), isNotNull);
        expect(await audit(), isNull);
      },
    );
  });
}
