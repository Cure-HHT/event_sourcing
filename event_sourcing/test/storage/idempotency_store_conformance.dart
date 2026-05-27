// Verifies: EVS-PRD-action-dispatch/D — backend-agnostic conformance
//   harness for the IdempotencyStore contract; concrete implementations
//   call this from their own test files. The harness is the canonical
//   place where lookup-miss, record-then-lookup-hit, tuple separation
//   ((action, principal, key)), expiry-on-lookup, and sweepExpired
//   semantics are exercised.
// Verifies: EVS-DEV-postgres-backend/F — both InMemoryIdempotencyStore
//   and PostgresIdempotencyStore pass this harness.
//
// This file MUST NOT register any `main()` of its own — it exposes one
// public function, [runIdempotencyStoreConformanceTests], which concrete
// stores call from their own test entrypoints.
//
// `setUp` calls the supplied factory; if it returns null (e.g., the
// Postgres harness with no `PG_TEST_URL`), the test marks itself
// skipped. The Postgres factory is responsible for truncating the
// `idempotency` table before constructing the store; the in-memory
// factory simply constructs a fresh `InMemoryIdempotencyStore()`.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

/// Run the backend-agnostic `IdempotencyStore` conformance suite against
/// the implementation produced by [factory].
///
/// [factory] is called in each test's `setUp` to produce a fresh, empty
/// store. Returning `null` from [factory] marks every test in the suite
/// as skipped (this is how the Postgres harness gates on `PG_TEST_URL`
/// absence).
///
/// [label] is the human-readable name folded into the outer group title
/// (e.g. `'in-memory'`, `'postgres'`).
void runIdempotencyStoreConformanceTests(
  Future<IdempotencyStore?> Function() factory, {
  required String label,
}) {
  group('IdempotencyStore conformance ($label)', () {
    late IdempotencyStore store;
    var initialized = false;

    setUp(() async {
      final candidate = await factory();
      if (candidate == null) {
        initialized = false;
        markTestSkipped('no IdempotencyStore available for $label');
        return;
      }
      store = candidate;
      initialized = true;
    });

    // Verifies: EVS-PRD-action-dispatch/D — lookup before record returns
    //   null (no cached outcome means the dispatcher must run the
    //   action).
    test('lookup miss returns null', () async {
      if (!initialized) return;
      final entry = await store.lookup('a', 'p', 'k');
      expect(entry, isNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — record-then-lookup returns
    //   the cached IdempotencyEntry verbatim (same resultJson and
    //   emittedEventIds), so the dispatcher can short-circuit a retry
    //   to the same outcome.
    test('record then lookup returns cached entry', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {'x': 1},
        emittedEventIds: const ['evt-1'],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      final entry = await store.lookup('a', 'p', 'k');
      expect(entry, isNotNull);
      expect(entry!.resultJson['x'], 1);
      expect(entry.emittedEventIds, ['evt-1']);
    });

    // Verifies: EVS-PRD-action-dispatch/E — record persists
    //   rawInputCanonicalJson alongside the cached outcome and lookup
    //   returns it, so the dispatcher can drive its content-mismatch
    //   check against any store implementation.
    test('record then lookup round-trips rawInputCanonicalJson', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {'x': 1},
        emittedEventIds: const ['evt-1'],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
        rawInputCanonicalJson: '{"a":1,"who":"original"}',
      );
      final entry = await store.lookup('a', 'p', 'k');
      expect(entry, isNotNull);
      expect(entry!.rawInputCanonicalJson, '{"a":1,"who":"original"}');
    });

    // Verifies: EVS-PRD-action-dispatch/E — omitting
    //   rawInputCanonicalJson at record time persists null (forward-
    //   compat: callers that don't supply canonical JSON, e.g. legacy
    //   rows or test harnesses, get a null on lookup; the dispatcher
    //   falls back to the plain cache-hit behavior).
    test(
      'record without rawInputCanonicalJson leaves it null on lookup',
      () async {
        if (!initialized) return;
        await store.record(
          actionName: 'a',
          principalId: 'p',
          key: 'k',
          resultJson: const {},
          emittedEventIds: const [],
          expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
          // rawInputCanonicalJson intentionally omitted
        );
        final entry = await store.lookup('a', 'p', 'k');
        expect(entry, isNotNull);
        expect(entry!.rawInputCanonicalJson, isNull);
      },
    );

    // Verifies: EVS-PRD-action-dispatch/D — entries are keyed by
    //   (action, principal, key); a different key for the same
    //   (action, principal) misses.
    test('lookup with different key misses', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      expect(await store.lookup('a', 'p', 'other'), isNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — entries are keyed by
    //   (action, principal, key); a different principal for the same
    //   (action, key) misses (principals do not share idempotency
    //   namespaces).
    test('lookup with different principal misses', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p1',
        key: 'k',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      expect(await store.lookup('a', 'p2', 'k'), isNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — entries are keyed by
    //   (action, principal, key); a different action for the same
    //   (principal, key) misses (action names do not share idempotency
    //   namespaces).
    test('lookup with different action misses', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      expect(await store.lookup('b', 'p', 'k'), isNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — sweepExpired removes
    //   entries with expires_at <= cutoff and returns the count of
    //   rows deleted; fresh entries survive.
    test('sweepExpired removes past entries', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'old',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2026-01-01T00:00:00Z'),
      );
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'fresh',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      final swept = await store.sweepExpired(
        before: DateTime.parse('2026-06-01T00:00:00Z'),
      );
      expect(swept, 1);
      expect(await store.lookup('a', 'p', 'old'), isNull);
      expect(await store.lookup('a', 'p', 'fresh'), isNotNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — lookup of an entry whose
    //   expires_at is <= now returns null even before sweepExpired has
    //   physically removed it; the cached-outcome window is closed at
    //   read time, not just at sweep time.
    test('expired lookup returns null even before sweep', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse('2026-01-01T00:00:00Z'),
      );
      final entry = await store.lookup(
        'a',
        'p',
        'k',
        now: DateTime.parse('2026-06-01T00:00:00Z'),
      );
      expect(entry, isNull);
    });

    // Verifies: EVS-PRD-action-dispatch/D — listEntries on an empty
    //   store returns an empty list (no entries means the inspector
    //   pane is correctly empty rather than rejecting the request).
    test('listEntries returns empty on empty store', () async {
      if (!initialized) return;
      final entries = await store.listEntries();
      expect(entries, isEmpty);
    });

    // Verifies: EVS-PRD-action-dispatch/D — listEntries after one
    //   record returns a single entry whose keying tuple matches the
    //   record arguments. Inspector UI relies on the entry carrying
    //   `(actionName, principalId, idempotencyKey)` directly so it can
    //   render the cache slot without a separate lookup.
    test('listEntries returns one entry after one record', () async {
      if (!initialized) return;
      await store.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const {'x': 1},
        emittedEventIds: const ['evt-1'],
        expiresAt: DateTime.parse('2099-01-01T00:00:00Z'),
      );
      final entries = await store.listEntries();
      expect(entries, hasLength(1));
      expect(entries.first.actionName, 'a');
      expect(entries.first.principalId, 'p');
      expect(entries.first.idempotencyKey, 'k');
      expect(entries.first.resultJson['x'], 1);
      expect(entries.first.emittedEventIds, ['evt-1']);
    });

    // Verifies: EVS-PRD-action-dispatch/D — listEntries returns every
    //   recorded entry, ordered by (actionName, principalId,
    //   idempotencyKey) ascending. The deterministic ordering is what
    //   makes the inspector UI's render stable across reloads.
    test('listEntries returns all entries in ascending key order', () async {
      if (!initialized) return;
      const expiry = '2099-01-01T00:00:00Z';
      await store.record(
        actionName: 'b',
        principalId: 'p1',
        key: 'k1',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse(expiry),
      );
      await store.record(
        actionName: 'a',
        principalId: 'p2',
        key: 'k2',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse(expiry),
      );
      await store.record(
        actionName: 'a',
        principalId: 'p1',
        key: 'k1',
        resultJson: const {},
        emittedEventIds: const [],
        expiresAt: DateTime.parse(expiry),
      );
      final entries = await store.listEntries();
      expect(entries, hasLength(3));
      expect(
        entries
            .map((e) => '${e.actionName}|${e.principalId}|${e.idempotencyKey}')
            .toList(),
        ['a|p1|k1', 'a|p2|k2', 'b|p1|k1'],
      );
    });
  });
}
