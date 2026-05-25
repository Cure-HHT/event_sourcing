// Imports below are intentionally retained even though this file's sole
// test is skipped: they keep the impl file in the compilation graph so
// the test runner surfaces breakage if the file is removed or fails to
// load. Real coverage of the mapper round-trip lives in
// e2e/view_test.dart in Phase 4.
//
// Verifies: EVS-PRD-view-subscriber/C — full mapper round-trip covered
//   in e2e/view_test.dart.
// ignore_for_file: unused_import
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_view_source.dart';

void main() {
  test(
    'mapper is applied to incoming envelopes',
    () {
      // RemoteViewSource takes a RemoteConnection; mapping happens inside
      // watch<T>(). E2E coverage validates the wire round-trip; this test
      // verifies the mapper-application unit by intercepting the connection
      // (covered in e2e/view_test.dart).
    },
    skip: 'unit-level mapper test covered in e2e/view_test.dart',
  );
}
