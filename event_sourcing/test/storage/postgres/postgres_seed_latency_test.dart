// A first boot's permission seed over a database reached across a network.
//
// A seed of some seventy grants that appended each in a transaction of its
// own waited once per grant for the lock the view catch-up holds on the
// table every append writes (EVS-DEV-view-convergence/L). On a database on
// the same host each wait is microseconds, so the seed took under two
// seconds; on a database reached over a network each wait ran to seconds,
// and the seed overran the budget a consumer gave its whole permission boot.
//
// This test reaches Postgres through a relay that delays every chunk in
// each direction, registers as many views as a deployed consumer does so
// the catch-up driver holds the lock as often, and seeds seventy grants on
// an empty log. The seed appends them in one transaction
// (EVS-DEV-bootstrap-action-permissions/E), which the test reads from the
// database itself: every grant event row carries the id of the transaction
// that inserted it, and the seed's rows carry one. The count does not
// depend on how fast the machine or the link is; the time the seed took is
// printed for comparison and asserted on by nothing.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:postgres/postgres.dart' show Sql;
import 'package:test/test.dart';

import 'latency_proxy.dart';
import 'test_postgres_url.dart';

const int _kGrantCount = 70;

/// Views a deployed portal registers beside the permission views, each on
/// an entry type of its own, so every append leaves them behind and the
/// catch-up driver runs a transaction per view after it.
const int _kOtherViewCount = 25;

/// Each direction of every round trip; a managed database in the same
/// region as its client answers in about this time. The driver makes
/// about three round trips per statement.
const Duration _kLatency = Duration(milliseconds: 2);

/// The deadline the seed is given: a guard against a hang, not a measure.
/// The assertion is the count of transactions the grants were inserted in.
const Duration _kSeedDeadline = Duration(minutes: 2);

String _seedYaml() {
  final buffer = StringBuffer('roles:\n  - admin\ngrants:\n  admin:\n');
  for (var i = 0; i < _kGrantCount; i++) {
    buffer.writeln('    - perm.$i');
  }
  return buffer.toString();
}

Set<Permission> _declared() => <Permission>{
  for (var i = 0; i < _kGrantCount; i++) Permission('perm.$i'),
};

String _otherType(int i) => 'other_note_$i';

Future<EventStore> _open(PostgresBackend backend) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry
    ..register(
      const EntryTypeDefinition(
        id: 'role_permission_grant',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Role-permission grant',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'User-role-scope assignment',
      ),
    );
  final projections = ProjectionRegistry()
    ..register(rolePermissionGrantsSpec)
    ..register(userRoleScopesSpec);
  for (var i = 0; i < _kOtherViewCount; i++) {
    registry.register(
      EntryTypeDefinition(
        id: _otherType(i),
        registeredVersion: const EntryTypeVersion(1, 0),
        name: _otherType(i),
      ),
    );
    projections.register(
      AggregateProjectionSpec(
        viewName: 'other_notes_$i',
        interest: SubscriptionFilter(entryTypes: <String>{_otherType(i)}),
        tombstoneEventTypes: const <String>{},
      ),
    );
  }
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      PostgresSecurityContextStore(backend: backend),
    ),
    entryTypes: registry,
    source: const Source(
      hopId: 'seed-latency-hop',
      identifier: 'seed-latency-install',
      softwareVersion: 'seed-latency-test',
    ),
    projections: projections,
  );
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'seedlat');
  if (db != null) tearDownAll(db.drop);
  LatencyProxy? proxy;
  EventStore? store;
  PostgresBackend? backend;

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
    final target = Uri.parse(db.adminUrl);
    proxy = await LatencyProxy.start(
      targetHost: target.host,
      targetPort: target.hasPort ? target.port : 5432,
      latency: _kLatency,
    );
  });

  tearDown(() async {
    await store?.close();
    store = null;
    await backend?.close();
    backend = null;
    await proxy?.close();
    proxy = null;
  });

  // Verifies: EVS-DEV-bootstrap-action-permissions/E
  // Verifies: EVS-DEV-bootstrap-action-permissions/A
  test('a first boot seeds $_kGrantCount grants in one transaction over a '
      'link with ${_kLatency.inMilliseconds} ms each way', () async {
    if (db == null) return;
    backend = await PostgresBackend.open(
      url: proxy!.rewrite(db.runtimeUrl),
      schema: db.schema,
      sslMode: SslMode.disable,
    );
    store = await _open(backend!);

    final seed = Stopwatch()..start();
    final result = await bootstrapActionPermissions(
      eventStore: store!,
      declaredPermissions: _declared(),
      yamlSource: _seedYaml(),
      timeout: _kSeedDeadline,
    );
    seed.stop();

    // ignore: avoid_print
    print(
      'seed latency: the seed of $_kGrantCount grants took ${seed.elapsed} '
      'over a link with ${_kLatency.inMilliseconds} ms each way',
    );

    expect(result, isA<PolicyReady>());
    final transactions = await db.asAdmin((admin) async {
      final found = await admin.execute(
        Sql.named(
          'SELECT count(DISTINCT xmin::text) AS transactions, count(*) AS grant_rows '
          'FROM ${quoteIdent(db.schema)}.events WHERE entry_type = @t',
        ),
        parameters: <String, Object?>{'t': 'role_permission_grant'},
      );
      return (found.single[0]! as int, found.single[1]! as int);
    });
    expect(
      transactions.$2,
      _kGrantCount,
      reason: 'the seed appends one grant event per declared grant',
    );
    expect(
      transactions.$1,
      1,
      reason:
          'the seed of $_kGrantCount grants inserted its events in '
          '${transactions.$1} transactions; one transaction per grant waits '
          'for the catch-up lock once per grant',
    );
    final rows = await store!.reader.findViewRows('role_permission_grants');
    expect(rows.rows, hasLength(_kGrantCount));
  }, timeout: const Timeout(Duration(minutes: 3)));
}
