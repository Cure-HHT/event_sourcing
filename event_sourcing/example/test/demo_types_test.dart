import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  group('demoNoteType (EntryTypeDefinition contract)', () {
    // demo_note declares id, registeredVersion and name. Constructor-
    // enforced, so this documents the demo's wiring rather than defending
    // a substrate obligation.
    test('id is "demo_note"', () {
      expect(demoNoteType.id, 'demo_note');
    });
    test('registeredVersion is positive', () {
      expect(demoNoteType.registeredVersion, greaterThan(0));
    });
    test('name non-empty', () {
      expect(demoNoteType.name, isNotEmpty);
    });
  });

  group('action-button entry types (RED / GREEN / BLUE)', () {
    // Each action-button type has its own id, and a non-'Note' aggregate
    // type stored separately in demoAggregateTypeByEntryTypeId
    // (EntryTypeDefinition itself carries no aggregateType field).
    test('redButtonType.id == "red_button_pressed"', () {
      expect(redButtonType.id, 'red_button_pressed');
    });
    test('greenButtonType.id == "green_button_pressed"', () {
      expect(greenButtonType.id, 'green_button_pressed');
    });
    test('blueButtonType.id == "blue_button_pressed"', () {
      expect(blueButtonType.id, 'blue_button_pressed');
    });
  });

  group('demoAggregateTypeByEntryTypeId (CQRS discriminator)', () {
    // Action-button events carry a non-'Note' aggregate type, which is what
    // gives the events panel its variant aggregate_type. That the notes view
    // actually excludes them is defended by the materializer routing test
    // below, not by these map lookups.
    test('demo_note maps to "Note"', () {
      expect(demoAggregateTypeByEntryTypeId['demo_note'], 'Note');
    });
    test('red_button_pressed maps to "RedButtonPressed"', () {
      expect(
        demoAggregateTypeByEntryTypeId['red_button_pressed'],
        'RedButtonPressed',
      );
    });
    test('green_button_pressed maps to "GreenButtonPressed"', () {
      expect(
        demoAggregateTypeByEntryTypeId['green_button_pressed'],
        'GreenButtonPressed',
      );
    });
    test('blue_button_pressed maps to "BlueButtonPressed"', () {
      expect(
        demoAggregateTypeByEntryTypeId['blue_button_pressed'],
        'BlueButtonPressed',
      );
    });
    test('all three action-button aggregate types are != "Note"', () {
      const actionIds = <String>[
        'red_button_pressed',
        'green_button_pressed',
        'blue_button_pressed',
      ];
      for (final id in actionIds) {
        expect(demoAggregateTypeByEntryTypeId[id], isNot('Note'));
      }
    });
  });

  group('allDemoEntryTypes (bootstrap registration)', () {
    // allDemoEntryTypes is a List<EntryTypeDefinition>; duplicate ids
    // would throw at registration time.
    test('has exactly four entries', () {
      expect(allDemoEntryTypes.length, 4);
    });
    test('all ids are unique', () {
      final ids = allDemoEntryTypes.map((e) => e.id).toSet();
      expect(ids.length, 4);
    });
    test('covers demo_note + three action-button types', () {
      final ids = allDemoEntryTypes.map((e) => e.id).toSet();
      expect(
        ids,
        containsAll(<String>{
          'demo_note',
          'red_button_pressed',
          'green_button_pressed',
          'blue_button_pressed',
        }),
      );
    });
  });

  group('materialization routing by aggregate type', () {
    // The demo's CQRS discriminator only means anything if the substrate
    // actually routes on it. Append one note and one of each action-button
    // event through a bootstrapped store, then read the notes view: the
    // note is folded in, the button events are not.
    // Verifies: EVS-PRD-materializer/A
    test('only demo_note events reach the notes view', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'demo-types-materializer.db',
      );
      final backend = SembastBackend(database: db);
      final projections = ProjectionRegistry()
        ..register(
          const AggregateProjectionSpec(
            viewName: 'notes',
            interest: SubscriptionFilter(entryTypes: <String>{'demo_note'}),
            tombstoneEventTypes: <String>{'tombstone'},
          ),
        );
      final datastore = await bootstrapEventStore(
        backend: backend,
        source: const Source(
          hopId: 'demo-types-test',
          identifier: '33333333-3333-4333-8333-333333333333',
          softwareVersion: 'test',
        ),
        entryTypes: allDemoEntryTypes,
        destinations: const <Destination>[],
        projections: projections,
      );

      await datastore.eventStore.append(
        entryType: 'demo_note',
        aggregateId: 'note-1',
        aggregateType: demoAggregateTypeByEntryTypeId['demo_note']!,
        eventType: 'finalized',
        data: const <String, Object?>{
          'answers': <String, Object?>{'title': 't', 'body': 'b'},
        },
        initiator: const UserInitiator('demo-user-1'),
      );
      for (final entryTypeId in <String>[
        'red_button_pressed',
        'green_button_pressed',
        'blue_button_pressed',
      ]) {
        await datastore.eventStore.append(
          entryType: entryTypeId,
          aggregateId: '$entryTypeId-1',
          aggregateType: demoAggregateTypeByEntryTypeId[entryTypeId]!,
          eventType: 'pressed',
          data: const <String, Object?>{},
          initiator: const UserInitiator('demo-user-1'),
        );
      }

      final rows = await backend.findViewRows('notes');
      expect(rows, hasLength(1));
      expect(rows.single['aggregateId'], 'note-1');

      await backend.close();
    });
  });
}
