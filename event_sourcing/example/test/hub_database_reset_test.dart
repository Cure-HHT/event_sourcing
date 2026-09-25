// The hub opens a database file an earlier data format wrote and shows a
// message naming the files to delete instead of crashing. App-side
// behaviour: carries no requirement citation.
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/database_reset_notice.dart';
import 'package:event_sourcing_demo/main.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;
import 'package:sembast/sembast_io.dart';

/// Writes [path] as a Sembast database an earlier data format wrote: one
/// event whose versions are single integers.
Future<void> _writeEarlierFormatDatabase(String path) async {
  final db = await databaseFactoryIo.openDatabase(path);
  final record = StoredEvent.synthetic(
    eventId: 'earlier-format-event',
    aggregateId: '_lib',
    aggregateType: '_lib',
    entryType: 'lib_version_initialized',
    eventType: 'lib_version_initialized',
    sequenceNumber: 1,
    eventHash: 'earlier-format-hash',
    initiator: const AutomationInitiator(service: 'event_sourcing'),
    clientTimestamp: DateTime.utc(2026, 3),
    data: const <String, dynamic>{'version': '0.3.1'},
  ).toMap();
  record['entry_type_version'] = 1;
  record['lib_format_version'] = 1;
  await intMapStoreFactory.store('events').add(db, record);
  await StoreRef<String, Object?>(
    'backend_state',
  ).record('sequence_counter').put(db, 1);
  await db.close();
}

void main() {
  test('a hub database from an earlier data format yields the reset '
      'message naming both database files', () async {
    final dir = await Directory.systemTemp.createTemp('hub_reset_');
    addTearDown(() => dir.delete(recursive: true));
    await _writeEarlierFormatDatabase(p.join(dir.path, 'demo_hub.db'));

    final app = await buildDemoApp(dir);

    expect(app, isA<DatabaseResetRequiredApp>());
    final message = (app as DatabaseResetRequiredApp).message;
    expect(message, contains(p.join(dir.path, 'demo.db')));
    expect(message, contains(p.join(dir.path, 'demo_hub.db')));
    expect(message, contains('Delete these files'));
    expect(message, contains('DatabaseResetRequiredError'));
  });

  test('a mobile database from an earlier data format yields the reset '
      'message after the hub opened', () async {
    final dir = await Directory.systemTemp.createTemp('hub_reset_');
    addTearDown(() => dir.delete(recursive: true));
    await _writeEarlierFormatDatabase(p.join(dir.path, 'demo.db'));

    final app = await buildDemoApp(dir);

    expect(app, isA<DatabaseResetRequiredApp>());
  });

  test('a database of another data-format major is not to be deleted: the '
      'message names the build to open it with, or a restore', () {
    final message = databaseResetMessage(
      DataFormatIncompatibleError(
        recordedPackageVersion: '9.0.0',
        recordedDataFormat: const DataFormatVersion(3, 0),
        packageVersion: LibVersion.version,
        dataFormat: LibVersion.dataFormat,
      ),
      <String>['demo.db', 'demo_hub.db'],
    );
    expect(message, isNot(contains('Delete')));
    expect(message, contains('data-format major it records'));
    expect(message, contains('restore'));
    expect(message, contains('demo_hub.db'));
  });

  testWidgets('the reset app shows the message', (tester) async {
    await tester.pumpWidget(
      const DatabaseResetRequiredApp(message: 'Delete these files: demo.db'),
    );
    expect(find.byKey(const Key('database-reset-message')), findsOneWidget);
    expect(find.textContaining('Delete these files'), findsOneWidget);
  });
}
