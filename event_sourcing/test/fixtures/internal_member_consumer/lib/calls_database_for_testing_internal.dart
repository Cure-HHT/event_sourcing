import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart'
    show SembastBackendTestSupport;

/// Reaches the raw sembast database through the test-support extension.
Object rawDatabase(SembastBackend backend) => backend.databaseForTesting;
