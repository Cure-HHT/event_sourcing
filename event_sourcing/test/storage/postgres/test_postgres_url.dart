// Verifies: EVS-DEV-postgres-backend/D
// URL-resolution helper for the
// conformance harness; returns PG_TEST_URL or null when unset.

import 'dart:io' show Platform;

/// Returns the Postgres URL the conformance harness should connect to,
/// or `null` when the test environment has not provided one. Tests that
/// receive `null` SHALL skip themselves rather than fail.
String? testPostgresUrl() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) return null;
  return url;
}
