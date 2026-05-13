// Implements: EVS-DEV-postgres-backend/D — conformance harness invocation
// helper. Returns the Postgres URL the harness should connect to, or
// null when the environment hasn't provided one (skip rather than fail).

import 'dart:io' show Platform;

/// Returns the Postgres URL the conformance harness should connect to,
/// or `null` when the test environment has not provided one. Tests that
/// receive `null` SHALL skip themselves rather than fail.
String? testPostgresUrl() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) return null;
  return url;
}
