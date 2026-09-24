// Verifies: EVS-PRD-subscription/C
// live subscribers of one event store receive its events in log order,
//   however the continuations of concurrent committed transactions resume:
//   concurrent appends on one store are delivered in sequence order, and
//   so are the events of a transaction that appends several.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/registry_with_audit.dart';

void main() {
  test('concurrent appends on one store are delivered in log order', () async {
    final db = await newDatabaseFactoryMemory().openDatabase('pub-order.db');
    final backend = SembastBackend(database: db);
    final deps = await buildAuditedRegistryDeps(
      backend,
      callerEntryTypes: const <EntryTypeDefinition>[
        EntryTypeDefinition(
          id: 'order_note',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'order_note',
        ),
      ],
    );
    final store = deps.eventStore;
    final delivered = <int>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value.sequenceNumber);
        });
    Future<void> appendOne(int round, int i) => store.append(
      entryType: 'order_note',
      aggregateId: 'a$round-$i',
      aggregateType: 'note',
      eventType: 'noted',
      data: <String, Object?>{'i': i},
      initiator: const UserInitiator('u'),
    );
    // Each committed transaction's continuation is held longer than the
    // next one's, so within a round the continuations resume in the reverse
    // of their commit order.
    var commits = 0;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        afterCommitBeforePublish: () async {
          final n = commits++;
          await Future<void>.delayed(Duration(milliseconds: (7 - n % 8) * 2));
        },
      ),
      () async {
        for (var round = 0; round < 20; round++) {
          await Future.wait(<Future<void>>[
            for (var i = 0; i < 8; i++) appendOne(round, i),
          ]);
        }
      },
    );
    expect(commits, 160, reason: 'every append passed the held seam');
    await pumpEventQueue();
    await sub.cancel();
    final stored = <int>[
      for (final e in await backend.findAllEvents())
        if (e.entryType == 'order_note') e.sequenceNumber,
    ];
    expect(delivered, stored, reason: 'delivered in sequence order');
    await backend.close();
  });

  test('a later commit whose continuation resumes first is delivered after '
      'the earlier one', () async {
    final db = await newDatabaseFactoryMemory().openDatabase('pub-late.db');
    final backend = SembastBackend(database: db);
    final deps = await buildAuditedRegistryDeps(
      backend,
      callerEntryTypes: const <EntryTypeDefinition>[
        EntryTypeDefinition(
          id: 'order_note',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'order_note',
        ),
      ],
    );
    final store = deps.eventStore;
    final delivered = <String>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value.aggregateId);
        });
    final late = Completer<void>();
    var calls = 0;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        // The first committed transaction's continuation resumes only after
        // the second one committed.
        afterCommitBeforePublish: () async {
          calls += 1;
          if (calls == 1) await late.future;
        },
      ),
      () async {
        Future<void> append(String id) => store.append(
          entryType: 'order_note',
          aggregateId: id,
          aggregateType: 'note',
          eventType: 'noted',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u'),
        );
        final first = append('first');
        while (calls == 0) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        await append('second');
        await pumpEventQueue();
        expect(delivered, isEmpty, reason: 'the second waits for the first');
        late.complete();
        await first;
      },
    );
    await pumpEventQueue();
    await sub.cancel();
    expect(delivered, <String>['first', 'second']);
    await backend.close();
  });

  test('the events of a transaction that appends several are delivered in '
      'log order, before a later commit whose continuation resumes '
      'first', () async {
    final db = await newDatabaseFactoryMemory().openDatabase('pub-multi.db');
    final backend = SembastBackend(database: db);
    final deps = await buildAuditedRegistryDeps(
      backend,
      callerEntryTypes: const <EntryTypeDefinition>[
        EntryTypeDefinition(
          id: 'order_note',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'order_note',
        ),
      ],
    );
    final store = deps.eventStore;
    final delivered = <StoredEvent>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value);
        });
    final late = Completer<void>();
    var calls = 0;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        // The multi-event transaction's continuation resumes only after the
        // single append committed.
        afterCommitBeforePublish: () async {
          calls += 1;
          if (calls == 1) await late.future;
        },
      ),
      () async {
        final several = store.runTransaction<void>((txn, collector) async {
          for (final id in const <String>['multi-1', 'multi-2', 'multi-3']) {
            await store.appendInTxn(
              txn,
              collector: collector,
              flowToken: null,
              metadata: null,
              security: null,
              checkpointReason: null,
              changeReason: null,
              dedupeByContent: false,
              entryType: 'order_note',
              aggregateId: id,
              aggregateType: 'note',
              eventType: 'noted',
              data: const <String, Object?>{},
              initiator: const UserInitiator('u'),
            );
          }
        });
        while (calls == 0) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        await store.append(
          entryType: 'order_note',
          aggregateId: 'single',
          aggregateType: 'note',
          eventType: 'noted',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u'),
        );
        await pumpEventQueue();
        expect(delivered, isEmpty, reason: 'the single waits for the several');
        late.complete();
        await several;
      },
    );
    await pumpEventQueue();
    await sub.cancel();
    expect(
      [for (final e in delivered) e.aggregateId],
      <String>['multi-1', 'multi-2', 'multi-3', 'single'],
    );
    final seqs = [for (final e in delivered) e.sequenceNumber];
    expect(seqs, [...seqs]..sort(), reason: 'delivered in sequence order');
    await backend.close();
  });
}
