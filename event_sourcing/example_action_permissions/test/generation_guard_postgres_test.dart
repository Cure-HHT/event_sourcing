// Verifies: EVS-DEV-version-compatibility/F
// a demo server instance of a build that raises the `demo_note` entry type
//   to a new major (2.0) is refused with IncompatibleGenerationException
//   while an instance of the 1.0 build is running on the same database,
//   and the running instance keeps serving and draining; once it has
//   stopped, the 2.0 build opens (a major bump is deployed
//   stop-then-start).
//
// Gated on PG_TEST_URL. Drops the demo schema and runs the demo's deployment
// step, so it runs one file at a time like every Postgres test; the
// instances connect as the declared runtime role.

@TestOn('vm')
library;

import 'dart:async';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/demo_bootstrap.dart';
import 'support/demo_postgres.dart';

Future<DemoServerComponents> _boot(
  PostgresBackend backend,
  String installIdentifier, {
  required List<String> delivered,
  Map<String, EntryTypeVersion> entryTypeVersions =
      const <String, EntryTypeVersion>{},
}) => bootstrapDemoServer(
  storage: demoStorageOver(backend),
  permissionsYaml: validPermissionsYaml,
  usersYaml: validUsersYaml,
  installIdentifier: installIdentifier,
  deliveryDestination: LogDestination(sink: delivered.add),
  entryTypeVersions: entryTypeVersions,
);

Future<void> _until(
  FutureOr<bool> Function() condition, {
  required String what,
}) async {
  final deadline = DateTime.now().add(const Duration(seconds: 30));
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out waiting for $what');
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}

Future<void> _note(EventStore store, String id) async {
  await store.append(
    entryType: 'demo_note',
    aggregateId: id,
    aggregateType: 'demo_note',
    eventType: 'note_written',
    data: <String, Object?>{'text': id},
    initiator: const UserInitiator('green-user-1'),
  );
}

void main() {
  final db = DemoPostgres.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping the generation guard test');
    });
    return;
  }

  test('a build raising an entry-type major is refused while the running '
      'build serves, and opens once it has stopped', () async {
    await db.reset();

    final deliveredOld = <String>[];
    final oldBackend = await db.open();
    final old = await _boot(
      oldBackend,
      'cccc0003-0000-4000-8000-0000000000c3',
      delivered: deliveredOld,
    );
    final oldCycle = await SyncCycle.start(
      registry: old.destinations,
      cadence: const Duration(milliseconds: 200),
    );
    var oldClosed = false;
    addTearDown(() async {
      if (oldClosed) return;
      await oldCycle.close();
      await old.eventStore.close();
    });

    // The 2.0 build is refused while the 1.0 build is connected.
    final newBackend = await db.open();
    addTearDown(newBackend.close);
    final eventsBefore = (await oldBackend.findAllEvents()).length;
    await expectLater(
      _boot(
        newBackend,
        'cccc0003-0000-4000-8000-0000000000c4',
        delivered: <String>[],
        entryTypeVersions: const <String, EntryTypeVersion>{
          'demo_note': EntryTypeVersion(2, 0),
        },
      ),
      throwsA(
        isA<IncompatibleGenerationException>().having(
          (e) => e.conflictingComponents.join(','),
          'conflictingComponents',
          contains('demo_note'),
        ),
      ),
    );
    expect((await oldBackend.findAllEvents()).length, eventsBefore);

    // The running build keeps serving and draining.
    await _note(old.eventStore, 'still-serving');
    await _until(
      () => deliveredOld.any((l) => l.contains('still-serving')),
      what: 'the running build to deliver',
    );
    expect(oldCycle.state, SyncCycleState.running);

    // Stop-then-start: once the 1.0 build has stopped, the 2.0 build opens.
    await oldCycle.close();
    await old.eventStore.close();
    oldClosed = true;
    final deliveredNew = <String>[];
    final upgraded = await _boot(
      newBackend,
      'cccc0003-0000-4000-8000-0000000000c4',
      delivered: deliveredNew,
      entryTypeVersions: const <String, EntryTypeVersion>{
        'demo_note': EntryTypeVersion(2, 0),
      },
    );
    addTearDown(upgraded.eventStore.close);
    final newCycle = await SyncCycle.start(
      registry: upgraded.destinations,
      cadence: const Duration(milliseconds: 200),
    );
    addTearDown(newCycle.close);
    await _note(upgraded.eventStore, 'after-upgrade');
    await _until(
      () => deliveredNew.any((l) => l.contains('after-upgrade')),
      what: 'the upgraded build to deliver',
    );
  }, timeout: const Timeout(Duration(minutes: 3)));
}
