// Backend-agnostic scenarios of the reconfigure-halt check, the refill
// guard, the configuration the drainer declares and records, and the
// persisted delivery status. Each test carries its own citations.
// Concrete backends run them from `test/sync/reconfigure_test.dart`
// (Sembast) and `test/storage/postgres/postgres_reconfigure_test.dart`
// (Postgres).
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'delivery_cycle_conformance.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'wedges_view_invariant.dart';

const Initiator _init = AutomationInitiator(service: 'reconfigure-scenarios');
const Initiator _operator = UserInitiator('operator-1');
const String _noteType = 'cycle_note';
const String _otherType = 'cycle_other';

/// Configuration C1: the destination receives notes and others.
Receiver c1(String id) => Receiver(
  id: id,
  entryTypes: const <String>{_noteType, _otherType},
  tag: 'c1',
);

/// Configuration C2: the filter narrowed to notes.
Receiver c2(String id) =>
    Receiver(id: id, entryTypes: const <String>{_noteType}, tag: 'c2');

/// A policy with no backoff and a budget of [n] attempts.
SyncPolicy _budget(int n) => SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: n,
);

/// Runs the reconfigure scenarios against the database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
void runReconfigureScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('reconfigure scenarios ($label)', () {
    late CycleWorld w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = CycleWorld(db);
      w.a = await w.openProcess();
    });

    tearDown(() async {
      if (!available) return;
      await w.closeAll();
    });

    /// A process registering [d] (activated once, through the first
    /// registry that registers it).
    Future<CycleProcess> processWith(Destination d) async {
      final p = await w.openProcess();
      await p.registry.addDestination(d, initiator: _init);
      final schedule = await p.registry.scheduleOf(d.id);
      if (schedule.startDate == null) {
        await p.registry.setStartDate(
          d.id,
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
      }
      return p;
    }

    /// Starts a hand-driven cycle over [p]: no append or registry operation
    /// wakes it.
    Future<SyncCycle> cycleOver(
      CycleProcess p, {
      String? configurationVersion,
      SyncPolicy? policy,
    }) async {
      final cycle = await w.start(
        on: p.registry,
        configurationVersion: configurationVersion,
        policy: policy ?? _budget(5),
      );
      p.store.deliveryTrigger = null;
      return cycle;
    }

    Future<StoredEvent> recoveryEvent() async => (await w.backend.findAllEvents(
      entryType: kDestinationWedgeRecoveredEntryType,
    )).last;

    Future<StoredEvent> wedgeEvent() async => (await w.backend.findAllEvents(
      entryType: kDestinationWedgedEntryType,
    )).last;

    Future<RefillGuard?> guard(String id) =>
        w.backend.transaction((txn) => w.backend.readRefillGuardTxn(txn, id));

    Future<RegistryCheck?> check() =>
        w.backend.transaction(w.backend.readRegistryCheckTxn);

    Future<String> otherEvent(String id, {EventStore? on}) async {
      final e = await (on ?? w.store).append(
        entryType: _otherType,
        aggregateId: id,
        aggregateType: 'other',
        eventType: 'noted',
        data: <String, Object?>{'id': id},
        initiator: const UserInitiator('u'),
      );
      return e!.eventId;
    }

    /// Everything a refused recovery must leave as it was: the queue, the
    /// fill position, the wedge record, the refill guard and the log.
    Future<Map<String, Object?>> snapshot(String id) async => {
      'rows': <Object?>[
        for (final r in await w.backend.listFifoEntries(id)) r.toJson(),
      ],
      'cursor': await w.backend.readFillCursor(id),
      'wedge': (await w.backend.transaction(
        (txn) => w.backend.readWedgeRecordTxn(txn, id),
      ))?.toJson(),
      'guard': (await guard(id))?.toJson(),
      'events': <String>[
        for (final e in await w.backend.findAllEvents()) e.eventId,
      ],
    };

    /// Halts [id] for [purpose] on process [p]'s cycle: requests the halt,
    /// appends a note and passes once, so the drainer wedges the head for
    /// the halt. Returns the wedged head's id.
    Future<String> haltedHead(
      CycleProcess p,
      SyncCycle cycle,
      String id, {
      HaltPurpose purpose = HaltPurpose.reconfigure,
    }) async {
      await p.registry.requestHalt(id, initiator: _operator, purpose: purpose);
      await w.note('halted-$id');
      await cycle();
      final head = (await w.backend.readFifoHead(id))!;
      expect(head.finalStatus, FinalStatus.wedged);
      return head.entryId;
    }

    Matcher refusedWith(String text) => throwsA(
      isA<StateError>().having((e) => e.message, 'message', contains(text)),
    );

    // Verifies: EVS-DEV-destination-drain/F
    // recovery of a reconfigure halt is refused while the lock holder
    //   declares the configuration recorded when the halt was honoured
    //   (the honouring drainer, and a later holder declaring the same
    //   configuration), writing only the registry check record; a holder
    //   declaring another configuration is accepted, the refill runs under
    //   it, and the recovery event records the epoch, the holder's
    //   configuration and its fingerprint, and the guard it set.
    // Verifies: EVS-PRD-destinations/M
    // the refusal leaves the wedged head as it was.
    test(
      'a reconfigure halt recovers only under a changed configuration',
      () async {
        if (!available) return;
        final pa = await processWith(c1('x'));
        final ca = await cycleOver(pa);
        await ca();
        final head = await haltedHead(pa, ca, 'x');
        final c1Fingerprint =
            (await wedgeEvent()).data['configuration_fingerprint'];
        expect(c1Fingerprint, isA<String>());
        final before = await snapshot('x');
        await expectLater(
          w.registry.tombstoneAndRefill('x', head, initiator: _operator),
          refusedWith('still declares the configuration'),
        );
        expect(await snapshot('x'), before);
        expect((await check())?.outcome, 'refused_configuration_unchanged');
        await ca.close();

        final pb = await processWith(c1('x'));
        final cb = await cycleOver(pb);
        await cb();
        final beforeB = await snapshot('x');
        await expectLater(
          w.registry.tombstoneAndRefill('x', head, initiator: _operator),
          refusedWith('still declares the configuration'),
        );
        expect(await snapshot('x'), beforeB);
        await cb.close();

        final receiver = c2('x');
        final pc = await processWith(receiver);
        final other = await otherEvent('o1');
        final cc = await cycleOver(pc);
        await cc();
        final result = await w.registry.tombstoneAndRefill(
          'x',
          head,
          initiator: _operator,
        );
        expect(result.rowId, head);
        final recovery = await recoveryEvent();
        final status = await pc.registry.readDeliveryStatus();
        expect(recovery.data['drainer_epoch'], await w.epoch());
        expect(
          recovery.data['drainer_configuration_fingerprint'],
          status.drainer!.fingerprints['x'],
        );
        expect(
          configurationFingerprint(
            Map<String, Object?>.from(
              recovery.data['drainer_configuration']! as Map,
            ),
          ),
          recovery.data['drainer_configuration_fingerprint'],
        );
        expect(recovery.data['refill_guard_fingerprint'], c1Fingerprint);
        expect((await guard('x'))?.fingerprint, c1Fingerprint);
        expect((await guard('x'))?.recoveryEventId, recovery.eventId);
        await cc();
        expect(
          await guard('x'),
          isNull,
          reason: 'the refill removed the guard',
        );
        expect(
          receiver.sentIds,
          isNot(contains(other)),
          reason: 'the refill ran under the narrowed filter',
        );
        expect(receiver.received, isNotEmpty);
        await expectWedgesViewMatchesQueue(w.store);
      },
    );

    // Verifies: EVS-DEV-destination-drain/F
    // an accepted recovery leaves a refill guard: a drainer that declares
    //   the recorded configuration does not refill and reports the
    //   destination unserved, which a process with no cycle reads through
    //   the persisted delivery status; a drainer declaring another
    //   configuration refills and removes the guard.
    // Verifies: EVS-DEV-destination-drain/T
    // the persisted delivery status shows the guard, the unserved reason
    //   and the current drainer's declaration, from a registry that
    //   registers nothing.
    test(
      'the refill guard holds a drainer of the halted configuration',
      () async {
        if (!available) return;
        final pa = await processWith(c1('x'));
        final ca = await cycleOver(pa);
        final head = await haltedHead(pa, ca, 'x');
        await ca.close();
        final pc = await processWith(c2('x'));
        final cc = await cycleOver(pc);
        await cc();
        await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
        await cc.close();

        final old = c1('x');
        final pd = await processWith(old);
        final cd = await cycleOver(pd);
        await cd();
        expect(old.started, isEmpty);
        expect(await w.backend.listFifoEntries('x'), hasLength(1));
        expect(cd.unserved, <String, UnservedReason>{
          'x': UnservedReason.refillAwaitsChangedConfiguration,
        });
        final reader = DestinationRegistry(
          eventStore: (await w.openProcess()).store,
        );
        final status = await reader.readDeliveryStatus();
        expect(status.drainer?.epoch, await w.epoch());
        expect(status.heartbeat?.epoch, await w.epoch());
        expect(status.destinations['x']?.refillGuard, isNotNull);
        expect(
          status.destinations['x']?.unserved,
          UnservedReason.refillAwaitsChangedConfiguration,
        );
        await cd.close();

        final fresh = c2('x');
        final pe = await processWith(fresh);
        final ce = await cycleOver(pe);
        await ce();
        expect(await guard('x'), isNull);
        expect(fresh.received, isNotEmpty, reason: 'refilled and delivered');
      },
    );

    // Verifies: EVS-DEV-destination-drain/F
    // the refill guard is removed only in the transaction that writes the
    //   refill: a refill whose transaction does not commit leaves the guard
    //   in place, so a drainer of the halted configuration that takes the
    //   lock next fills nothing; at no point is the guard gone while
    //   nothing has been refilled.
    test('a refill that does not commit leaves the guard in place', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final pc = await processWith(c2('x'));
      final cc = await cycleOver(pc);
      await cc();
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
      final rowsAfterRecovery = await w.backend.listFifoEntries('x');
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(failFillTransaction: (id) => id == 'x'),
        cc.call,
      );
      expect(await guard('x'), isNotNull);
      expect(await w.backend.listFifoEntries('x'), rowsAfterRecovery);
      await cc.close();

      final old = c1('x');
      final pd = await processWith(old);
      final cd = await cycleOver(pd);
      await cd();
      expect(old.started, isEmpty);
      expect(await w.backend.listFifoEntries('x'), rowsAfterRecovery);
      expect(cd.unserved, <String, UnservedReason>{
        'x': UnservedReason.refillAwaitsChangedConfiguration,
      });
      await cd.close();

      final violations = <String>[];
      final fresh = c2('x');
      final pe = await processWith(fresh);
      final ce = await cycleOver(pe);
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterFillReads: (id) async {
            if (id != 'x') return;
            final pending = <FifoEntry>[
              for (final r in await w.backend.listFifoEntries('x'))
                if (r.finalStatus == null) r,
            ];
            if (await guard('x') == null && pending.isEmpty) {
              violations.add('the guard is gone and nothing was refilled');
            }
          },
        ),
        ce.call,
      );
      expect(violations, isEmpty);
      expect(await guard('x'), isNull);
      expect(fresh.received, isNotEmpty);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // the refill guard stays until the refill has passed the position the
    //   recovery rewound from: a drainer of the halted configuration that
    //   takes the lock part way through a refill of several items does not
    //   continue it, and the drainer of the changed configuration finishes
    //   it and removes the guard.
    test('a takeover part way through a refill does not continue it', () async {
      if (!available) return;
      final first = c1('x')
        ..outcome = (_) => const SendTransient(error: 'busy');
      final pa = await processWith(first);
      final ca = await cycleOver(pa, policy: _budget(20));
      final notes = <String>[for (var i = 0; i < 3; i++) await w.note('m$i')];
      for (var i = 0; i < 3; i++) {
        await ca();
      }
      expect(await w.backend.listFifoEntries('x'), hasLength(3));
      await pa.registry.requestHalt(
        'x',
        initiator: _operator,
        purpose: HaltPurpose.reconfigure,
      );
      await ca();
      final head = (await w.backend.readFifoHead('x'))!;
      expect(head.finalStatus, FinalStatus.wedged);
      await ca.close();

      final pc = await processWith(c2('x'));
      final cc = await cycleOver(pc);
      await cc();
      final cursorBefore = await w.backend.readFillCursor('x');
      await w.registry.tombstoneAndRefill(
        'x',
        head.entryId,
        initiator: _operator,
      );
      final lastSeq = (await w.backend.findEventById(
        notes.last,
      ))!.sequenceNumber;
      expect((await guard('x'))?.refillThrough, cursorBefore);
      expect(cursorBefore, greaterThanOrEqualTo(lastSeq));
      await cc();
      expect(await guard('x'), isNotNull, reason: 'one of three refilled');
      await cc.close();

      final old = c1('x');
      final pd = await processWith(old);
      final cd = await cycleOver(pd);
      final rows = await w.backend.listFifoEntries('x');
      await cd();
      expect(old.started, isEmpty);
      expect(await w.backend.listFifoEntries('x'), hasLength(rows.length));
      expect(cd.unserved['x'], UnservedReason.refillAwaitsChangedConfiguration);
      await cd.close();

      final fresh = c2('x');
      final pe = await processWith(fresh);
      final ce = await cycleOver(pe);
      // Two more items, then a pass that moves the position past the
      // events the filter rejects, up to where the recovery rewound from.
      for (var i = 0; i < 3; i++) {
        await ce();
      }
      expect(await guard('x'), isNull);
      expect(<String>{...fresh.sentIds}, containsAll(notes.skip(1)));
    });

    // Verifies: EVS-DEV-destination-drain/F
    // the ways out of a guard that stays in place: a drainer restarted with
    //   a changed configuration version refills and removes it; a deletion
    //   removes it.
    test('the ways out of a guard that stays in place', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final pc = await processWith(c2('x'));
      final cc = await cycleOver(pc);
      await cc();
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
      await cc.close();

      final pd = await processWith(c1('x'));
      final cd = await cycleOver(pd);
      await cd();
      expect(await guard('x'), isNotNull);
      await cd.close();
      final cd2 = await cycleOver(pd, configurationVersion: 'rollback-2');
      await cd2();
      expect(await guard('x'), isNull, reason: 'the fill cleared it');
      await cd2.close();

      // A second destination with a guard in place is deleted.
      final py = await processWith(c1('y'));
      final cy = await cycleOver(py);
      final yHead = await haltedHead(py, cy, 'y');
      await cy.close();
      final pz = await processWith(c2('y'));
      final cz = await cycleOver(pz);
      await cz();
      await w.registry.tombstoneAndRefill('y', yHead, initiator: _operator);
      await cz.close();
      expect(await guard('y'), isNotNull);
      await w.registry.deleteDestination('y', initiator: _operator);
      expect(await guard('y'), isNull);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // a drainer whose filter lists the same sets in another order, and
    //   whose hard-delete opt-in differs, declares the same configuration:
    //   recovery of the reconfigure halt is refused under it.
    test('an equal declaration built differently is refused', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final reordered = Receiver(
        id: 'x',
        entryTypes: const <String>{_otherType, _noteType},
        allowHardDelete: false,
        tag: 'reordered',
      );
      final pb = await processWith(reordered);
      final cb = await cycleOver(pb);
      await cb();
      await expectLater(
        w.registry.tombstoneAndRefill('x', head, initiator: _operator),
        refusedWith('still declares the configuration'),
      );
    });

    // Verifies: EVS-DEV-destination-drain/F
    // a changed configuration version alone, with the same declared
    //   fields, is a changed configuration.
    test('a changed configuration version alone is accepted', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa, configurationVersion: 'v1');
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final cb = await cycleOver(pa, configurationVersion: 'v2');
      await cb();
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
      final declared =
          (await recoveryEvent()).data['drainer_configuration']! as Map;
      expect(declared['configuration_version'], 'v2');
    });

    // Verifies: EVS-DEV-destination-drain/F
    // when the new configuration was deployed before the halt, recovery of
    //   a reconfigure halt is refused with a message naming both ways out;
    //   a restart with a changed configuration version recovers it, and a
    //   pause halt under the deployed configuration recovers with no
    //   restart.
    test('the deploy-first order', () async {
      if (!available) return;
      final pa = await processWith(c2('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      final refusal = expectLater(
        w.registry.tombstoneAndRefill('x', head, initiator: _operator),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            allOf(
              contains('deploy it first'),
              contains('restart the drainer with a changed'),
              contains('purpose pause'),
            ),
          ),
        ),
      );
      await refusal;
      await ca.close();
      final cb = await cycleOver(pa, configurationVersion: 'restarted');
      await cb();
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
      await cb();
      final pauseHead = await haltedHead(
        pa,
        cb,
        'x',
        purpose: HaltPurpose.pause,
      );
      await w.registry.tombstoneAndRefill('x', pauseHead, initiator: _operator);
      expect((await recoveryEvent()).data['refill_guard_fingerprint'], isNull);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // right after the lock changed hands, before the new holder's first
    //   pass, recovery of a reconfigure halt is refused until it declares.
    test('recovery right after a takeover waits for a pass', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final pc = await processWith(c2('x'));
      final cc = await cycleOver(pc);
      await expectLater(
        w.registry.tombstoneAndRefill('x', head, initiator: _operator),
        refusedWith('retry after its next pass'),
      );
      expect(
        (await check())?.outcome,
        'refused_no_declaration_since_lock_changed',
      );
      await cc();
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // a pause halt recovers under the same holder and leaves no guard.
    test('a pause halt recovers with the same holder', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x', purpose: HaltPurpose.pause);
      await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
      expect(await guard('x'), isNull);
      expect((await recoveryEvent()).data['refill_guard_fingerprint'], isNull);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // a reconfigure request consumed by a permanent-refusal wedge, and one
    //   consumed by an exhausted budget, are checked as a reconfigure halt:
    //   refused under the same configuration, accepted under a changed one,
    //   which leaves the guard.
    // Verifies: EVS-DEV-destination-drain/O
    // the failure wedge consumes the request and keeps its purpose.
    test('a reconfigure request consumed by a failure wedge', () async {
      if (!available) return;
      for (final cause in <String>[
        'permanent_refusal',
        'retry_budget_exhausted',
      ]) {
        final id = 'f-$cause';
        final r = c1(id);
        final gate = Completer<void>();
        r
          ..gate = (() => gate.future)
          ..outcome = (_) => cause == 'permanent_refusal'
              ? const SendPermanent(error: 'no')
              : const SendTransient(error: 'busy');
        final pa = await processWith(r);
        final ca = await cycleOver(pa, policy: _budget(1));
        await w.note('n-$id');
        final pass = ca();
        await until(() => r.started.isNotEmpty, reason: 'the send');
        await pa.registry.requestHalt(
          id,
          initiator: _operator,
          purpose: HaltPurpose.reconfigure,
        );
        gate.complete();
        await pass;
        final wedge = await wedgeEvent();
        expect(wedge.data['cause'], cause);
        expect(wedge.data['halt_purpose'], 'reconfigure');
        final head = wedge.data['row_id']! as String;
        final before = await snapshot(id);
        await expectLater(
          w.registry.tombstoneAndRefill(id, head, initiator: _operator),
          refusedWith('still declares the configuration'),
        );
        expect(await snapshot(id), before);
        await ca.close();
        final pb = await processWith(c2(id));
        final cb = await cycleOver(pb);
        await cb();
        await w.registry.tombstoneAndRefill(id, head, initiator: _operator);
        expect(await guard(id), isNotNull);
        await cb.close();
      }
    });

    // Verifies: EVS-DEV-destination-drain/F
    // recovery of a reconfigure halt is refused while no drainer serves the
    //   destination; the destination can be deleted instead, which ends
    //   the wedge.
    test('no drainer serves the destination', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa);
      final head = await haltedHead(pa, ca, 'x');
      await ca.close();
      final cn = await cycleOver(await w.openProcess());
      await cn();
      final before = await snapshot('x');
      await expectLater(
        w.registry.tombstoneAndRefill('x', head, initiator: _operator),
        refusedWith('no drainer serves this destination'),
      );
      expect(await snapshot('x'), before);
      await w.registry.deleteDestination('x', initiator: _operator);
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readWedgeRecordTxn(txn, 'x'),
        ),
        isNull,
      );
      await expectWedgesViewMatchesQueue(w.store);
    });

    // Verifies: EVS-DEV-destination-drain/F
    // an accepted recovery under a lock holder that does not register the
    //   destination records the holder's epoch and no holder configuration,
    //   fingerprint or guard.
    test(
      'a recovery under a holder that does not serve the destination',
      () async {
        if (!available) return;
        final pa = await processWith(c1('x'));
        final ca = await cycleOver(pa);
        final head = await haltedHead(pa, ca, 'x', purpose: HaltPurpose.pause);
        await ca.close();
        final cn = await cycleOver(await w.openProcess());
        await cn();
        await w.registry.tombstoneAndRefill('x', head, initiator: _operator);
        final recovery = await recoveryEvent();
        expect(recovery.data['drainer_epoch'], await w.epoch());
        expect(recovery.data['drainer_configuration'], isNull);
        expect(recovery.data['drainer_configuration_fingerprint'], isNull);
        expect(recovery.data['refill_guard_fingerprint'], isNull);
        expect(await guard('x'), isNull);
      },
    );

    // Verifies: EVS-DEV-destination-drain/F
    // an accepted reconfigure recovery whose event append fails commits
    //   nothing: no refill guard, the head still wedged, the queue and the
    //   fill position as they were, no event.
    test(
      'a reconfigure recovery that does not commit leaves no guard',
      () async {
        if (!available) return;
        final pa = await processWith(c1('x'));
        final ca = await cycleOver(pa);
        final head = await haltedHead(pa, ca, 'x');
        await ca.close();
        final pc = await processWith(c2('x'));
        final cc = await cycleOver(pc);
        await cc();
        final before = await snapshot('x');
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) =>
                  t == kDestinationWedgeRecoveredEntryType,
            ),
            () =>
                w.registry.tombstoneAndRefill('x', head, initiator: _operator),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await snapshot('x'), before);
        expect(await guard('x'), isNull);
      },
    );

    // Verifies: EVS-DEV-destination-drain/I
    // the wedge event records the drain epoch, the declared configuration
    //   (with the configuration version the cycle was started with) and its
    //   fingerprint, which the configuration hashes to.
    test('the wedge event records the declared configuration', () async {
      if (!available) return;
      final pa = await processWith(c1('x'));
      final ca = await cycleOver(pa, configurationVersion: 'build-7');
      await haltedHead(pa, ca, 'x');
      final wedge = await wedgeEvent();
      expect(wedge.data['drainer_epoch'], await w.epoch());
      final configuration = Map<String, Object?>.from(
        wedge.data['configuration']! as Map,
      );
      expect(configuration['configuration_version'], 'build-7');
      expect(
        configurationFingerprint(configuration),
        wedge.data['configuration_fingerprint'],
      );
      final record = await w.backend.transaction(
        (txn) => w.backend.readWedgeRecordTxn(txn, 'x'),
      );
      expect(record?.drainerEpoch, wedge.data['drainer_epoch']);
      expect(
        record?.configurationFingerprint,
        wedge.data['configuration_fingerprint'],
      );
    });

    // Verifies: EVS-DEV-destination-drain/T
    // the persisted delivery status, read from a registry that registers
    //   nothing in a process with no cycle: no drainer before any pass;
    //   then the drainer's declaration and heartbeat, an open halt request,
    //   a wedge record, and the unserved reasons the drainer's latest pass
    //   recorded.
    test('the persisted delivery status', () async {
      if (!available) return;
      final reader = DestinationRegistry(
        eventStore: (await w.openProcess()).store,
      );
      final empty = await reader.readDeliveryStatus();
      expect(empty.drainer, isNull);
      expect(empty.heartbeat, isNull);

      final wedged = c1('wedged');
      final pa = await processWith(wedged);
      final remote = c1('remote');
      final pb = await processWith(remote);
      await pb.registry.requestHalt(
        'remote',
        initiator: _operator,
        purpose: HaltPurpose.pause,
      );
      final gone = c1('gone');
      await pa.registry.addDestination(gone, initiator: _init);
      await pb.registry.deleteDestination('gone', initiator: _operator);
      final ca = await cycleOver(pa);
      await haltedHead(pa, ca, 'wedged');
      final status = await reader.readDeliveryStatus();
      expect(status.drainer?.epoch, await w.epoch());
      expect(status.heartbeat?.epoch, await w.epoch());
      expect(status.drainer?.unserved, <String, UnservedReason>{
        'remote': UnservedReason.notRegisteredHere,
        'gone': UnservedReason.deletedInStorage,
      });
      expect(
        status.destinations['wedged']?.wedge?.cause,
        WedgeCause.operatorHalt,
      );
      expect(status.destinations['remote']?.openHaltRequest, isNotNull);
      expect(
        status.destinations['remote']?.unserved,
        UnservedReason.notRegisteredHere,
      );
      expect(status.destinations.containsKey('gone'), isFalse);
      expect(ca.unserved, status.drainer?.unserved);
    });

    // Verifies: EVS-DEV-destination-drain/T
    // the persisted delivery status reports no current drainer, and no
    //   unserved reason, while the stored declaration is from an earlier
    //   drain epoch (the lock changed hands and the new holder has not
    //   passed yet); after the new holder's pass it reports that holder's
    //   declaration. The heartbeat record carries its own epoch.
    test(
      'a declaration of an earlier epoch is not the current drainer',
      () async {
        if (!available) return;
        final pa = await processWith(c1('local'));
        await processWith(c1('remote'));
        final ca = await cycleOver(pa);
        await ca();
        final reader = DestinationRegistry(
          eventStore: (await w.openProcess()).store,
        );
        final before = await reader.readDeliveryStatus();
        expect(before.drainer?.epoch, await w.epoch());
        expect(
          before.destinations['remote']?.unserved,
          UnservedReason.notRegisteredHere,
        );
        final epochA = await w.epoch();
        await ca.close();

        final lock = await w.backend.tryAcquireDrainLock(
          databaseId: w.store.databaseId,
        );
        expect(lock.epoch, greaterThan(epochA!));
        final taken = await reader.readDeliveryStatus();
        expect(taken.drainer, isNull);
        expect(taken.destinations['remote']?.unserved, isNull);
        expect(taken.heartbeat?.epoch, epochA);
        await lock.release();

        final cb = await cycleOver(pa);
        await cb();
        final after = await reader.readDeliveryStatus();
        expect(after.drainer?.epoch, await w.epoch());
        expect(after.drainer!.epoch, greaterThan(lock.epoch));
        expect(
          after.destinations['remote']?.unserved,
          UnservedReason.notRegisteredHere,
        );
      },
    );

    // Verifies: EVS-DEV-destination-drain/T
    // a destination the draining registry holds under another registration
    //   than the database's (another process deleted it and registered it
    //   again) is not filled under the stale configuration: it is reported
    //   unserved, in the cycle and in the persisted delivery status, with no
    //   configuration declared for it; registering it again in the draining
    //   process serves it.
    test('a destination registered again elsewhere is not served', () async {
      if (!available) return;
      final stale = c1('x');
      final pa = await processWith(stale);
      final ca = await cycleOver(pa);
      await w.note('before');
      await ca();
      expect(stale.received, isNotEmpty);
      final pb = await w.openProcess();
      await pb.registry.deleteDestination('x', initiator: _operator);
      final renewed = c2('x');
      await pb.registry.addDestination(renewed, initiator: _init);
      await pb.registry.setStartDate(
        'x',
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      final rowsBefore = await w.backend.listFifoEntries('x');
      final other = await otherEvent('o1');
      await ca();
      expect(ca.unserved['x'], UnservedReason.registrationMismatch);
      expect(await w.backend.listFifoEntries('x'), rowsBefore);
      final status = await pb.registry.readDeliveryStatus();
      expect(
        status.destinations['x']?.unserved,
        UnservedReason.registrationMismatch,
      );
      expect(status.drainer!.fingerprints.containsKey('x'), isFalse);

      await pa.registry.addDestination(renewed, initiator: _init);
      await ca();
      expect(ca.unserved.containsKey('x'), isFalse);
      expect(renewed.received, isNotEmpty);
      expect(renewed.sentIds, isNot(contains(other)));
    });

    // Verifies: EVS-PRD-destinations/V
    // delivery uses the destinations registered in the process that
    //   drains: a destination registered only elsewhere gets nothing and is
    //   reported unserved, until a cycle over the registering process
    //   drains.
    // Verifies: EVS-DEV-destination-drain/E
    // the draining process's registry decides what is filled and sent.
    test('a destination registered only in another process', () async {
      if (!available) return;
      final pa = await processWith(c1('local'));
      final x = c1('x');
      final pb = await processWith(x);
      final id = await w.note('n1');
      final ca = await cycleOver(pa);
      await ca();
      expect(await w.backend.listFifoEntries('x'), isEmpty);
      expect(ca.unserved['x'], UnservedReason.notRegisteredHere);
      await ca.close();
      final cb = await cycleOver(pb);
      await cb();
      expect(x.sentIds, contains(id));
    });
  });
}
