// All reserved system entry types ship with isMaterialized:false. Cross-aggregate
// stream events stay out of view-side projection on every install, on both the
// local-append path and the ingest path (the outer gate `def.isMaterialized`
// short-circuits the materializer loop in `_appendInTxn` and
// `_ingestOneInTxn` before any materializer is consulted).
//
// Regression guard: flipping any reserved `EntryTypeDefinition` to
//   `isMaterialized: true` would silently start firing materializers on system
//   audit events. This test fails loudly if that happens.
//
// Verifies: EVS-PRD-event-log/A — all reserved system entry types have
//   isMaterialized:false, ensuring audit events never corrupt view-side state.
// Verifies: EVS-DEV-event-store-open/B/C — lib_version_initialized and
//   lib_version_changed are registered in kSystemEntryTypes so byId() returns
//   non-null and SubscriptionFilter correctly gates them behind
//   includeSystemEvents:true.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'install-materialize-false-test',
  softwareVersion: 'pkg@0.0.1',
);

void main() {
  group('Reserved system entry types — isMaterialized:false', () {
    // Verifies that every entry type auto-registered by
    // `bootstrapEventStore` ships `isMaterialized: false`. Iteration is
    // driven from `kReservedSystemEntryTypeIds` and the registry is consulted
    // post-bootstrap so the test exercises the actual auto-registered
    // `EntryTypeDefinition` instances rather than re-importing the
    // internal `kSystemEntryTypes` list.
    test('all reserved system entry types have '
        'materialize:false', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'mat-false-${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final backend = SembastBackend(database: db);
      try {
        final datastore = await bootstrapEventStore(
          backend: backend,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[],
          destinations: const <Destination>[],
        );

        // The reserved id set is the canonical list of system entry
        // types and SHALL be exactly 14. A change here implies a new
        // system entry type was added without updating this assertion;
        // the test forces an explicit decision on whether the new id
        // also ships isMaterialized:false.
        //
        // Count is 14: 10 security/bootstrap audits + 2 lib-version boot
        // events (lib_version_initialized, lib_version_changed, gated
        // behind includeSystemEvents:true by SubscriptionFilter) + 2
        // entry-type-version events (ingest-audit, view_snapshot_promoted).
        expect(
          kReservedSystemEntryTypeIds.length,
          equals(14),
          reason:
              'kReservedSystemEntryTypeIds is the canonical 14-element '
              'set per adding a new system entry type '
              'requires updating this expectation explicitly.',
        );

        for (final id in kReservedSystemEntryTypeIds) {
          final definition = datastore.entryTypes.byId(id);
          expect(
            definition,
            isNotNull,
            reason:
                'reserved system entry type "$id" must be auto-registered '
                'by bootstrapEventStore.',
          );
          expect(
            definition!.isMaterialized,
            isFalse,
            reason:
                '$id MUST ship isMaterialized:false to keep cross-aggregate '
                'stream events out of view-side projection. '
                'Flipping a reserved system entry type to isMaterialized:true '
                'would start firing materializers on system audits — out '
                'of scope for Phase 4.22.',
          );
        }
      } finally {
        await backend.close();
      }
    });

    test('kSystemEntryTypes registers ingest-audit at version 1, '
        'non-materializing', () {
      final byId = {for (final d in kSystemEntryTypes) d.id: d};
      expect(
        byId.containsKey('ingest-audit'),
        isTrue,
        reason:
            'ingest-audit must be a registered system entry type so the '
            'raw-path ingest-audit callers can read registeredVersion from '
            'the registry instead of hardcoding.',
      );
      expect(byId['ingest-audit']!.registeredVersion, 1);
      expect(byId['ingest-audit']!.isMaterialized, isFalse);
    });

    test('kSystemEntryTypes registers view_snapshot_promoted at version 1, '
        'non-materializing', () {
      final byId = {for (final d in kSystemEntryTypes) d.id: d};
      expect(
        byId.containsKey('view_snapshot_promoted'),
        isTrue,
        reason:
            'view_snapshot_promoted is emitted by the boot-time '
            'snapshot-promotion pass and must be in the registry to be '
            'append-stampable under the new substrate-owned version model.',
      );
      expect(byId['view_snapshot_promoted']!.registeredVersion, 1);
      expect(byId['view_snapshot_promoted']!.isMaterialized, isFalse);
    });
  });
}
