// Runs the scenarios of outstanding_finding_mark_conformance.dart on
// Sembast (in memory), and checks that a projection naming a key, column
// or derived field beginning with `$` is refused at registration.
//
// The scenarios' assertions are cited on their own tests in
// test_support/outstanding_finding_mark_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/outstanding_finding_mark_conformance.dart';
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

class _SembastDatabase implements VersionTestDatabase {
  _SembastDatabase(this._db);

  final Database _db;

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _counter = 0;

const SubscriptionFilter _interest = SubscriptionFilter(
  entryTypes: <String>{'note'},
);

Matcher _refusalNaming(String name) => isA<ArgumentError>().having(
  (e) => e.message.toString(),
  'message',
  contains(name),
);

void main() {
  runOutstandingFindingMarkScenarios(
    openDatabase: () async {
      _counter += 1;
      return _SembastDatabase(
        await newDatabaseFactoryMemory().openDatabase(
          'outstanding-finding-mark-$_counter.db',
        ),
      );
    },
    backendLabel: 'sembast',
  );

  group(r'registration of a projection naming a $ field', () {
    // Verifies: EVS-PRD-materializer/H
    test(r'a derived field beginning with $ is refused by name', () {
      final registry = ProjectionRegistry();
      expect(
        () => registry.register(
          const AggregateProjectionSpec(
            viewName: 'v',
            interest: _interest,
            tombstoneEventTypes: <String>{},
            derivedFields: <DerivedField>[
              DerivedField(
                r'$derived',
                DottedPathLookup('a', fallback: ConstantValue(0)),
              ),
            ],
          ),
        ),
        throwsA(_refusalNaming(r'$derived')),
      );
      expect(registry.lookup('v'), isNull);
    });

    // Verifies: EVS-PRD-materializer/H
    test(r'a table key path beginning with $ is refused by name', () {
      final registry = ProjectionRegistry();
      expect(
        () => registry.register(
          const TableProjectionSpec(
            viewName: 'v',
            interest: _interest,
            insertEventTypes: <String>{'x'},
            removeEventTypes: <String>{},
            rowKey: CompositeKey(<String>[r'data.$key']),
            rowData: WholePayload(),
          ),
        ),
        throwsA(_refusalNaming(r'$key')),
      );
      expect(registry.lookup('v'), isNull);
    });

    // Verifies: EVS-PRD-materializer/H
    test(r'a table column beginning with $ is refused by name', () {
      final registry = ProjectionRegistry();
      for (final rowData in const <RowDataExtractor>[
        SelectedFields(<String>['ok', r'$column']),
        PayloadField(r'$column'),
      ]) {
        expect(
          () => registry.register(
            TableProjectionSpec(
              viewName: 'v',
              interest: _interest,
              insertEventTypes: const <String>{'x'},
              removeEventTypes: const <String>{},
              rowKey: const AggregateIdKey(),
              rowData: rowData,
            ),
          ),
          throwsA(_refusalNaming(r'$column')),
        );
      }
      expect(registry.lookup('v'), isNull);
    });
  });
}
