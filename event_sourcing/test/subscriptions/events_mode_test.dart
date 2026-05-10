import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<EventStore> _openStore() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'em-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  await EventStore.open(storage: backend);

  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'X',
        registeredVersion: 1,
        name: 'X',
        widgetId: 'w',
        widgetConfig: <String, Object?>{},
      ),
    );

  return EventStore(
    backend: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-instance',
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: ProjectionRegistry(),
    promoters: PromoterRegistry(),
  );
}

void main() {
  test(
    'Events mode delivers subsequent appends, not pre-existing history',
    () async {
      final store = await _openStore();

      Future<void> append(String aggId, String type) => store.append(
        entryType: 'X',
        entryTypeVersion: 1,
        aggregateId: aggId,
        aggregateType: 'X',
        eventType: type,
        data: const <String, Object?>{},
        initiator: const UserInitiator('u'),
      );

      await append('a', 'before'); // history; not delivered

      final received = <StoredEvent>[];
      final sub = store
          .subscribe<StoredEvent>(
            SubscriptionFilter(aggregateTypes: const {'X'}),
            const Events(),
          )
          .listen((Update<StoredEvent> u) {
            if (u is Delta<StoredEvent>) received.add(u.value);
          });

      await append('a', 'after1');
      await append('b', 'after2');
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(received.map((e) => e.eventType).toList(), ['after1', 'after2']);
      await sub.cancel();
      await store.close();
    },
  );
}
