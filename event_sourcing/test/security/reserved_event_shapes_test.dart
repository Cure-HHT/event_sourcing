// Verifies: EVS-DEV-destination-drain/L
// the library declares, for every reserved system entry type, the one
//   aggregate type and the event types it appends that entry type with; no
//   two reserved entry types share a declared pair, so the pair identifies
//   the entry type; the declared shapes are fixed within a data-format
//   major; and the check every reserved append of the library runs before
//   it writes refuses a shape the library does not declare, and a
//   destination audit whose data ingest would refuse.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        checkReservedAppend,
        kDestinationAuditAggregateType,
        kDestinationAuditEntryTypes,
        kIngestAuditAggregateType,
        kIngestAuditEntryType,
        kIngestDeliveryAcceptedEventType,
        kReservedEventShapes;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

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
/// The values of the enumerated fields of reserved events that a build of
/// each data-format major knows, by major. A later release of the major may
/// add a value (a minor step: every build of the major stores and folds an
/// event carrying a value it does not know verbatim), but never removes or
/// renames one, so every value recorded for the running major is one this
/// build knows.
const Map<int, Map<String, List<String>>> _enumeratedValuesByDataFormatMajor =
    <int, Map<String, List<String>>>{
      2: <String, List<String>>{
        'cause': <String>[
          'operator_halt',
          'permanent_refusal',
          'retry_budget_exhausted',
        ],
        'purpose': <String>['pause', 'reconfigure'],
      },
      3: <String, List<String>>{
        'cause': <String>[
          'acknowledgement_invalid',
          'operator_halt',
          'permanent_refusal',
          'retry_budget_exhausted',
        ],
        'purpose': <String>['pause', 'reconfigure'],
        'kind': <String>[
          'channel_unexplained',
          'delivery_hash_mismatch',
          'event_malformed',
          'foreign_event',
          'fork_unrecorded',
          'hash_mismatch',
          'identity_mismatch',
          'own_event_ingested',
          'parent_invalid',
          'parents_not_stamped',
          'position_reused',
          'predecessor_break',
          'restore_unverified',
          'sender_regressed',
          'sequence_missing',
          'storage_link_break',
          'succession_ahead',
        ],
      },
    };

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
        'system.destination_halt_requested': <Object>[
          'system_destination',
          <String>['destination_halt_requested'],
        ],
        'system.destination_halt_cancelled': <Object>[
          'system_destination',
          <String>['destination_halt_cancelled'],
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
      3: <String, List<Object>>{
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
        'system.destination_halt_requested': <Object>[
          'system_destination',
          <String>['destination_halt_requested'],
        ],
        'system.destination_halt_cancelled': <Object>[
          'system_destination',
          <String>['destination_halt_cancelled'],
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
          <String>['ingest.delivery_accepted', 'ingest.duplicate_received'],
        ],
        'view_snapshot_promoted': <Object>[
          '_lib',
          <String>['finalized'],
        ],
        'system.security_finding': <Object>[
          'security_finding',
          <String>['security_finding_recorded'],
        ],
        'system.destination_channel_resumed': <Object>[
          'system_destination',
          <String>['destination_channel_resumed'],
        ],
        'system.destination_sender_succeeded': <Object>[
          'system_destination',
          <String>['destination_sender_succeeded'],
        ],
      },
    };

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

    // Verifies: EVS-DEV-causal-parents/E
    test('every event type of every reserved entry type is declared an '
        'ineligible annotation', () {
      final byId = <String, EntryTypeDefinition>{
        for (final d in kSystemEntryTypes) d.id: d,
      };
      for (final shape in kReservedEventShapes.entries) {
        final definition = byId[shape.key]!;
        final declared = <String>{
          for (final d in definition.declarations) d.eventType,
        };
        expect(declared, shape.value.eventTypes, reason: shape.key);
        for (final eventType in shape.value.eventTypes) {
          final declaration = definition.declarationFor(eventType);
          expect(
            declaration.kind,
            CausalKind.annotation,
            reason: '${shape.key}/$eventType',
          );
          expect(
            declaration.eligible,
            isFalse,
            reason: '${shape.key}/$eventType',
          );
        }
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
        kDestinationHaltRequestedEntryType: kDestinationHaltRequestedEventType,
        kDestinationHaltCancelledEntryType: kDestinationHaltCancelledEventType,
        kDestinationChannelResumedEntryType:
            kDestinationChannelResumedEventType,
        kDestinationSenderSucceededEntryType:
            kDestinationSenderSucceededEventType,
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

    // Verifies: EVS-DEV-destination-drain/L
    // an enumerated value of a reserved event is never removed or renamed
    //   within a data-format major: this build knows every value recorded
    //   for its major.
    test('this build knows every enumerated value recorded for its '
        'data-format major', () {
      final recorded =
          _enumeratedValuesByDataFormatMajor[LibVersion.dataFormat.major];
      expect(
        recorded,
        isNotNull,
        reason:
            'record the enumerated field values of data-format major '
            '${LibVersion.dataFormat.major}',
      );
      final known = <String, List<String>>{
        'cause': <String>[for (final c in WedgeCause.values) c.wire],
        'purpose': <String>[for (final p in HaltPurpose.values) p.wire],
        'kind': <String>[for (final k in FindingKind.values) k.wire],
      };
      expect(known.keys.toSet(), recorded!.keys.toSet());
      for (final field in recorded.entries) {
        expect(known[field.key], containsAll(field.value), reason: field.key);
      }
    });

    // Verifies: EVS-DEV-delivery-receiver/S
    // ingest.delivery_accepted is an event type of the reserved ingest audit
    //   entry type.
    test('the ingest audit declares the duplicate-received and the '
        'delivery-accepted event types; no reserved shape admits '
        'ingest.batch_rejected', () {
      expect(kReservedEventShapes[kIngestAuditEntryType]!.eventTypes, <String>{
        'ingest.duplicate_received',
        'ingest.delivery_accepted',
      });
      expect(kIngestDeliveryAcceptedEventType, 'ingest.delivery_accepted');
      for (final shape in kReservedEventShapes.values) {
        expect(
          shape.admits(shape.aggregateType, 'ingest.batch_rejected'),
          isFalse,
        );
      }
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

  group('delivery channel reserved declarations', () {
    const newTypes = <String, (String, String)>{
      kIngestAuditEntryType: (
        kIngestAuditAggregateType,
        kIngestDeliveryAcceptedEventType,
      ),
      kDestinationChannelResumedEntryType: (
        kDestinationAuditAggregateType,
        kDestinationChannelResumedEventType,
      ),
      kDestinationSenderSucceededEntryType: (
        kDestinationAuditAggregateType,
        kDestinationSenderSucceededEventType,
      ),
    };

    // Verifies: EVS-DEV-resume-event/H
    // the resume event and the succession event are reserved destination
    //   audit entry types, system.destination_channel_resumed and
    //   system.destination_sender_succeeded, each with its own event type.
    test('the resume and succession events are reserved destination audits '
        'with event types of their own', () {
      expect(
        kDestinationChannelResumedEntryType,
        'system.destination_channel_resumed',
      );
      expect(
        kDestinationSenderSucceededEntryType,
        'system.destination_sender_succeeded',
      );
      expect(
        kDestinationChannelResumedEventType,
        isNot(kDestinationSenderSucceededEventType),
      );
      for (final id in <String>[
        kDestinationChannelResumedEntryType,
        kDestinationSenderSucceededEntryType,
      ]) {
        expect(isReservedEntryType(id), isTrue, reason: id);
        expect(kReservedSystemEntryTypeIds, contains(id));
        expect(kDestinationAuditEntryTypes, contains(id));
      }
    });

    // Verifies: EVS-DEV-delivery-receiver/S
    // ingest.delivery_accepted belongs to the reserved ingest audit, which
    //   the public append operations refuse.
    test('the public append refuses each new reserved type', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'reserved-channel-${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final backend = SembastBackend(database: db);
      addTearDown(backend.close);
      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(
          backend,
          SembastSecurityContextStore(backend: backend),
        ),
        entryTypes: EntryTypeRegistry(),
        source: const Source(
          hopId: 'server',
          identifier: 'channel-install',
          softwareVersion: 'test@1.0.0',
        ),
      );
      addTearDown(store.close);
      for (final entry in newTypes.entries) {
        await expectLater(
          store.append(
            entryType: entry.key,
            aggregateId: 'a',
            aggregateType: entry.value.$1,
            eventType: entry.value.$2,
            data: const <String, Object?>{'id': 'x', 'database_id': 'db'},
            initiator: const UserInitiator('u'),
          ),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              contains('reserved entry-type namespace'),
            ),
          ),
          reason: entry.key,
        );
        expect(await store.reader.findAllEvents(entryType: entry.key), isEmpty);
      }
    });

    test('the reserved append checks the shape of each new type', () {
      for (final entry in newTypes.entries) {
        checkReservedAppend(
          entryType: entry.key,
          aggregateType: entry.value.$1,
          eventType: entry.value.$2,
          data: const <String, Object?>{'id': 'x', 'database_id': 'db'},
        );
        expect(
          () => checkReservedAppend(
            entryType: entry.key,
            aggregateType: 'note',
            eventType: entry.value.$2,
            data: const <String, Object?>{'id': 'x', 'database_id': 'db'},
          ),
          throwsArgumentError,
          reason: entry.key,
        );
      }
      // A per-channel ingest audit aggregate id is admitted: the shape
      // constrains the aggregate type and event type only.
      expect(
        kReservedEventShapes[kIngestAuditEntryType]!.admits(
          kIngestAuditAggregateType,
          kIngestDeliveryAcceptedEventType,
        ),
        isTrue,
      );
      for (final id in <String>[
        kDestinationChannelResumedEntryType,
        kDestinationSenderSucceededEntryType,
      ]) {
        expect(
          () => checkReservedAppend(
            entryType: id,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kReservedEventShapes[id]!.eventTypes.single,
            data: const <String, Object?>{'id': 'x'},
          ),
          throwsArgumentError,
          reason: '$id without a database identity',
        );
      }
    });

    // Verifies: EVS-PRD-destinations/Q
    // the wedge event can record an acceptance that carries no receiver
    //   record as its cause.
    test('the acknowledgement_invalid wedge cause', () {
      expect(WedgeCause.acknowledgementInvalid.wire, 'acknowledgement_invalid');
      expect(WedgeCause.values, contains(WedgeCause.acknowledgementInvalid));
      expect(
        WedgeCause.fromWire('acknowledgement_invalid'),
        WedgeCause.acknowledgementInvalid,
      );
      expect(WedgeCause.acknowledgementInvalid.isKnown, isTrue);
    });
  });

  group('the library appends reserved events only in a declared shape', () {
    void expectRefused(
      String entryType,
      String aggregateType,
      String eventType,
      Map<String, Object?> data,
      String message,
    ) {
      expect(
        () => checkReservedAppend(
          entryType: entryType,
          aggregateType: aggregateType,
          eventType: eventType,
          data: data,
        ),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message.toString(),
            'message',
            contains(message),
          ),
        ),
      );
    }

    test('an undeclared event type is refused', () {
      expectRefused(
        kDestinationWedgedEntryType,
        kDestinationAuditAggregateType,
        kDestinationDeletedEventType,
        const <String, Object?>{'id': 'x', 'database_id': 'db'},
        'declares',
      );
    });

    test('an undeclared aggregate type is refused', () {
      expectRefused(
        kDestinationWedgedEntryType,
        'note',
        kDestinationWedgedEventType,
        const <String, Object?>{'id': 'x', 'database_id': 'db'},
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
      test('a destination audit with ${c.key} is refused', () {
        expectRefused(
          kDestinationWedgeRecoveredEntryType,
          kDestinationAuditAggregateType,
          kDestinationWedgeRecoveredEventType,
          c.value,
          'destination audit',
        );
      });
    }

    test('an entry type that is not reserved is refused', () {
      expectRefused(
        _noteType,
        'note',
        'finalized',
        const <String, Object?>{},
        'not a reserved',
      );
    });

    test('a declared shape with well-formed data passes', () {
      checkReservedAppend(
        entryType: kDestinationRegisteredEntryType,
        aggregateType: kDestinationAuditAggregateType,
        eventType: kDestinationRegisteredEventType,
        data: const <String, Object?>{'id': 'x', 'database_id': 'db'},
      );
    });

    test('the registry audit appends a declared shape and dedupes by '
        'content', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'reserved-shapes-${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final backend = SembastBackend(database: db);
      addTearDown(backend.close);
      Future<void> boot() async {
        final bundle = await bootstrapEventStore(
          storage: ApplicationSuppliedStorage(
            backend,
            SembastSecurityContextStore(backend: backend),
          ),
          source: const Source(
            hopId: 'server',
            identifier: 'shapes-install',
            softwareVersion: 'test@1.0.0',
          ),
          entryTypes: const <EntryTypeDefinition>[
            EntryTypeDefinition(
              id: _noteType,
              registeredVersion: EntryTypeVersion(1, 0),
              name: _noteType,
            ),
          ],
          destinations: const <Destination>[],
        );
        await bundle.eventStore.close();
      }

      await boot();
      await boot();
      final audits = await backend.findAllEvents(
        entryType: kEntryTypeRegistryInitializedEntryType,
      );
      expect(audits, hasLength(1), reason: 'the unchanged registry dedupes');
      final shape =
          kReservedEventShapes[kEntryTypeRegistryInitializedEntryType]!;
      expect(
        shape.admits(audits.single.aggregateType, audits.single.eventType),
        isTrue,
      );
    });
  });
}
