import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Note {
  final String entryId;
  final Map<String, Object?> answers;
  _Note({required this.entryId, required this.answers});
  static _Note fromMap(Map<String, Object?> m) => _Note(
    entryId: m['latestEventId'] as String? ?? '_',
    answers: (m['answers'] as Map?)?.cast<String, Object?>() ?? {},
  );
}

Future<EventStore> _open() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'am-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: 1,
        name: 'Epistaxis Event',
      ),
    );
  final projections = ProjectionRegistry()
    ..register(
      AggregateProjectionSpec(
        viewName: 'diary_entries',
        interest: const SubscriptionFilter(aggregateTypes: {'note'}),
        tombstoneEventTypes: const {'tombstone'},
      ),
    );
  return EventStore.open(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-instance',
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: projections,
    promoters: PromoterRegistry(),
  );
}

Future<void> _append(
  EventStore store,
  String aggId,
  String type, [
  Map<String, Object?>? data,
]) => store.append(
  entryType: 'epistaxis_event',
  aggregateId: aggId,
  aggregateType: 'note',
  eventType: type,
  data: data ?? const <String, Object?>{},
  initiator: const UserInitiator('u'),
);

void main() {
  test(
    'snapshot for not-yet-existing aggregate emits null-value Snapshot',
    () async {
      final store = await _open();
      final updates = <Update<_Note?>>[];
      final sub = store
          .subscribe(
            const SubscriptionFilter(),
            AggregateMode<_Note?>(
              viewName: 'diary_entries',
              mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
              aggregates: const {'never-created'},
            ),
          )
          .listen(updates.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(updates.length, 1);
      expect(updates.first, isA<Snapshot<_Note?>>());
      expect((updates.first as Snapshot).value, isNull);
      await sub.cancel();
      await store.close();
    },
  );

  test(
    'snapshot for existing aggregate carries current state; subsequent appends emit Delta',
    () async {
      final store = await _open();
      await _append(store, 'e1', 'finalized', {
        'answers': {'q1': 'yes'},
      });

      final updates = <Update<_Note?>>[];
      final sub = store
          .subscribe(
            const SubscriptionFilter(),
            AggregateMode<_Note?>(
              viewName: 'diary_entries',
              mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
              aggregates: const {'e1'},
            ),
          )
          .listen(updates.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));

      // Snapshot delivered
      expect(updates.first, isA<Snapshot<_Note?>>());
      expect((updates.first as Snapshot).value, isNotNull);

      await _append(store, 'e1', 'checkpoint', {
        'answers': {'q2': 'no'},
      });
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(updates.any((u) => u is Delta<_Note?>), isTrue);
      final delta = updates.whereType<Delta<_Note?>>().last;
      expect(delta.value!.answers, {'q1': 'yes', 'q2': 'no'});
      await sub.cancel();
      await store.close();
    },
  );

  test('snapshot sequence reflects latest folded event sequence', () async {
    final store = await _open();
    await _append(store, 'e1', 'finalized', {
      'answers': {'q1': 'yes'},
    });
    final stored2 = await store.append(
      entryType: 'epistaxis_event',
      aggregateId: 'e1',
      aggregateType: 'note',
      eventType: 'checkpoint',
      data: {
        'answers': {'q2': 'no'},
      },
      initiator: const UserInitiator('u'),
    );

    final updates = <Update<_Note?>>[];
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
            aggregates: const {'e1'},
          ),
        )
        .listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.first, isA<Snapshot<_Note?>>());
    expect((updates.first as Snapshot).sequence, stored2!.sequenceNumber);
    await sub.cancel();
    await store.close();
  });

  test('tombstone produces Tombstone update for active subscribers', () async {
    final store = await _open();
    await _append(store, 'e1', 'finalized', {
      'answers': {'q1': 'yes'},
    });

    final updates = <Update<_Note?>>[];
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
            aggregates: const {'e1'},
          ),
        )
        .listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    await _append(store, 'e1', 'tombstone');
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.any((u) => u is Tombstone<_Note?>), isTrue);
    await sub.cancel();
    await store.close();
  });
}
