import 'package:event_sourcing/event_sourcing.dart';

/// Opens an event store through the test-only open, which records no
/// library-version event, from production code.
Future<EventStore> openWithoutRecord(SembastBackend backend) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: EntryTypeRegistry(),
      source: const Source(
        hopId: 'consumer',
        identifier: 'consumer-install',
        softwareVersion: 'consumer@1',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
