// One library instance in a process of its own, for the Postgres generation
// guard test's process-exit case: it opens a backend and an event store
// registering one entry type at 1.0 on the database at the URL given as its
// argument, prints `ready`, and waits to be killed. Declares no requirement
// behaviour of its own.
import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';

Future<void> main(List<String> args) async {
  final backend = await PostgresBackend.open(
    url: args.single,
    sslMode: SslMode.disable,
  );
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: 'guard_x',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'guard_x',
    ),
  );
  await EventStore.open(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'guard-process',
      identifier: 'guard-process',
      softwareVersion: 'guard-test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
  );
  stdout.writeln('ready');
  await Completer<void>().future;
}
