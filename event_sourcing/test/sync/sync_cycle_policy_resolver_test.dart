// Verifies: EVS-DEV-destination-drain/I
// (the wedge event records the retry budget in effect: the budget a
//   cycle resolves once per pass, a static policy's budget, or the default
//   budget when neither supplies one, reaches every destination's drain)
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';
import '../test_support/registry_with_audit.dart';

const Initiator _testInit = AutomationInitiator(service: 'test-bootstrap');

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

Future<({SembastBackend backend, DestinationRegistry registry})>
_bootstrap() async {
  final backend = await _openBackend(
    'sync-cycle-resolver-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final deps = await buildAuditedRegistryDeps(backend);
  final registry = DestinationRegistry(eventStore: deps.eventStore);
  return (backend: backend, registry: registry);
}

Future<String> _enqueueOne(
  SembastBackend backend,
  String destId,
  String eventId, {
  int sequenceNumber = 1,
}) async {
  final entry = await enqueueSingle(
    backend,
    destId,
    eventId: eventId,
    sequenceNumber: sequenceNumber,
    wirePayload: <String, Object?>{'who': destId, 'which': eventId},
    wireFormat: 'fake-v1',
    transformVersion: 'fake-v1',
  );
  return entry.entryId;
}

/// A policy whose budget differs from [SyncPolicy.defaults], so a drain
/// that used the defaults instead would behave observably differently.
SyncPolicy _budget(int maxAttempts) => SyncPolicy(
  initialBackoff: const Duration(seconds: 1),
  backoffMultiplier: 1.0,
  maxBackoff: const Duration(seconds: 1),
  jitterFraction: 0,
  maxAttempts: maxAttempts,
);

List<SendResult> _transients(int n) => <SendResult>[
  for (var i = 0; i < n; i++) const SendTransient(error: 'busy'),
];

Future<List<StoredEvent>> _wedgeEvents(
  SembastBackend backend,
  String destId,
) async => <StoredEvent>[
  for (final e in await backend.findAllEvents())
    if (e.entryType == kDestinationWedgedEntryType && e.data['id'] == destId) e,
];

/// Runs [cycle] once per step, calling [advanceClock] between steps so
/// the next step is past the backoff, until the head of [dest] is wedged. Asserts each step sends exactly once and the head stays pending
/// until the last, and returns the number of sends that wedged it.
Future<int> _sendsUntilWedged(
  SyncCycle cycle,
  SembastBackend backend,
  FakeDestination dest,
  String entryId,
  void Function() advanceClock, {
  int limit = 40,
}) async {
  for (var step = 1; step <= limit; step++) {
    await cycle();
    expect(dest.sent, hasLength(step), reason: 'one send per cycle');
    final row = (await backend.readFifoRow(dest.id, entryId))!;
    expect(row.attempts, hasLength(step));
    if (row.finalStatus == FinalStatus.wedged) return step;
    expect(row.finalStatus, isNull, reason: 'pending until the budget');
    expect(await _wedgeEvents(backend, dest.id), isEmpty);
    advanceClock();
  }
  fail('the head of ${dest.id} did not wedge within $limit sends');
}

/// Asserts the single wedge event of [destId] records a spent budget of
/// [maxAttempts] after [maxAttempts] attempts.
Future<void> _expectBudgetWedge(
  SembastBackend backend,
  String destId,
  int maxAttempts,
) async {
  final event = (await _wedgeEvents(backend, destId)).single;
  expect(event.data['cause'], 'retry_budget_exhausted');
  expect(event.data['attempt_count'], maxAttempts);
  expect(event.data['max_attempts'], maxAttempts);
}

void main() {
  group('policyResolver invocation', () {
    test('resolver called exactly once per call()', () async {
      final ctx = await _bootstrap();
      var calls = 0;
      final cycle = await SyncCycle.start(
        registry: ctx.registry,
        cadence: const Duration(hours: 1),
        policyResolver: () {
          calls += 1;
          return SyncPolicy.defaults;
        },
      );
      await cycle();
      expect(calls, 1);
      await cycle();
      expect(calls, 2);
      await cycle.close();
      await ctx.backend.close();
    });

    // The policy resolved for a pass reaches every destination's drain:
    // the resolver returns a budget of one, which differs from the default
    // budget, and every destination's head wedges on its first transient
    // send with that budget recorded, from a single resolver call.
    test(
      'resolver result is the same across all destinations within one cycle',
      () async {
        final ctx = await _bootstrap();

        final dests = <FakeDestination>[
          for (final id in const <String>['a', 'b', 'c'])
            FakeDestination(id: id, script: _transients(1)),
        ];
        final entryIds = <String, String>{};
        for (final d in dests) {
          await ctx.registry.addDestination(d, initiator: _testInit);
          entryIds[d.id] = await _enqueueOne(ctx.backend, d.id, 'e1');
        }

        var calls = 0;
        final cycle = await SyncCycle.start(
          registry: ctx.registry,
          cadence: const Duration(hours: 1),
          clock: () => DateTime.utc(2026, 4, 22, 10),
          policyResolver: () {
            calls += 1;
            return _budget(1);
          },
        );

        await cycle();

        expect(calls, 1);
        for (final d in dests) {
          expect(d.sent, hasLength(1), reason: d.id);
          final row = (await ctx.backend.readFifoRow(d.id, entryIds[d.id]!))!;
          expect(row.finalStatus, FinalStatus.wedged, reason: d.id);
          expect(row.attempts, hasLength(1), reason: d.id);
          await _expectBudgetWedge(ctx.backend, d.id, 1);
        }

        await cycle.close();
        await ctx.backend.close();
      },
    );

    // A resolver that returns null leaves the drain on SyncPolicy.defaults:
    // the head is sent once per cycle until the default budget is spent,
    // and the wedge records that budget.
    test('resolver returning null falls back to SyncPolicy.defaults', () async {
      await _expectDefaultBudget(policyResolver: () => null);
    });
  });

  group('mutual exclusivity + throws', () {
    // Supplying both policy and policyResolver throws ArgumentError at
    // construction time.
    test(
      'constructing with both policy and policyResolver throws ArgumentError',
      () async {
        final ctx = await _bootstrap();
        await expectLater(
          SyncCycle.start(
            registry: ctx.registry,
            policy: SyncPolicy.defaults,
            policyResolver: () => SyncPolicy.defaults,
          ),
          throwsArgumentError,
        );
        await ctx.backend.close();
      },
    );

    // Verifies: EVS-DEV-destination-drain/F
    // the configuration version the log records with every wedge and
    //   recovery is a bounded identifier: an empty, over-long or free-text
    //   value is refused before anything starts, and an identifier of the
    //   maximum length is accepted.
    test(
      'a configuration version that is not an identifier is refused',
      () async {
        final ctx = await _bootstrap();
        for (final bad in <String>[
          '',
          'x' * (SyncCycle.maxConfigurationVersionLength + 1),
          'build 7',
          'line\nbreak',
          'secret=abc',
        ]) {
          await expectLater(
            SyncCycle.start(registry: ctx.registry, configurationVersion: bad),
            throwsArgumentError,
            reason: bad,
          );
        }
        final cycle = await SyncCycle.start(
          registry: ctx.registry,
          cadence: const Duration(hours: 1),
          configurationVersion:
              'a' * (SyncCycle.maxConfigurationVersionLength - 12) +
              '1.2+b:c@d/e_',
        );
        await cycle.close();
        await ctx.backend.close();
      },
    );

    // When the resolver throws, the cycle aborts (exception propagates),
    // the reentrancy guard is cleared via try/finally, and a subsequent
    // trigger may invoke call() again.
    test('resolver throws → cycle aborts; reentrancy guard cleared', () async {
      final ctx = await _bootstrap();
      var first = true;
      final cycle = await SyncCycle.start(
        registry: ctx.registry,
        cadence: const Duration(hours: 1),
        policyResolver: () {
          if (first) {
            first = false;
            throw StateError('boom');
          }
          return SyncPolicy.defaults;
        },
      );
      await expectLater(cycle(), throwsStateError);
      // The failed call left the cycle running; this call must succeed.
      expect(cycle.state, SyncCycleState.running);
      await cycle();
      await cycle.close();
      await ctx.backend.close();
    });
  });

  group('regression', () {
    // SyncCycle with neither policy nor resolver drains under
    // SyncPolicy.defaults.
    test(
      'SyncCycle with neither policy nor resolver still works (defaults)',
      () async {
        await _expectDefaultBudget();
      },
    );

    // When an explicit policy is supplied with no resolver, the field is
    // forwarded to drain unchanged: its budget, not the default one, wedges
    // the head and is recorded in the wedge event.
    test(
      'SyncCycle with explicit policy: still uses it (today behavior)',
      () async {
        final ctx = await _bootstrap();
        final dest = FakeDestination(id: 'x', script: _transients(3));
        await ctx.registry.addDestination(dest, initiator: _testInit);
        final entryId = await _enqueueOne(ctx.backend, 'x', 'e1');
        var now = DateTime.utc(2026, 4, 22, 10);
        final cycle = await SyncCycle.start(
          registry: ctx.registry,
          cadence: const Duration(hours: 1),
          clock: () => now,
          policy: _budget(3),
        );
        final sends = await _sendsUntilWedged(
          cycle,
          ctx.backend,
          dest,
          entryId,
          () => now = now.add(const Duration(minutes: 1)),
        );
        expect(sends, 3);
        await _expectBudgetWedge(ctx.backend, 'x', 3);
        await cycle.close();
        await ctx.backend.close();
      },
    );
  });
}

/// Drives a destination whose every send is transient through a cycle
/// started with [policyResolver] (or with neither a policy nor a resolver)
/// and asserts the head wedges after exactly the default budget of sends.
Future<void> _expectDefaultBudget({
  SyncPolicy? Function()? policyResolver,
}) async {
  final budget = SyncPolicy.defaults.maxAttempts;
  final ctx = await _bootstrap();
  final dest = FakeDestination(id: 'x', script: _transients(budget));
  await ctx.registry.addDestination(dest, initiator: _testInit);
  final entryId = await _enqueueOne(ctx.backend, 'x', 'e1');
  var now = DateTime.utc(2026, 4, 22, 10);
  final cycle = await SyncCycle.start(
    registry: ctx.registry,
    cadence: const Duration(hours: 1),
    clock: () => now,
    policyResolver: policyResolver,
  );
  // The default curve caps at 2 h plus 10% jitter; 3 h clears it.
  final sends = await _sendsUntilWedged(
    cycle,
    ctx.backend,
    dest,
    entryId,
    () => now = now.add(const Duration(hours: 3)),
  );
  expect(sends, budget);
  await _expectBudgetWedge(ctx.backend, 'x', budget);
  await cycle.close();
  await ctx.backend.close();
}
