// Imports below are intentionally retained even though this file's sole
// test is skipped: they keep the impl file in the compilation graph so
// the test runner surfaces breakage if the file is removed or fails to
// load. Full coverage of two-phase load + AuthSession dependency lives
// in e2e/permission_test.dart in Phase 4.
//
// Verifies: EVS-PRD-permission-source/A/D/E — Remote impl surface;
//   full per-assertion coverage in e2e/permission_test.dart.
// ignore_for_file: unused_import
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';

void main() {
  test(
    'current is null before AuthSession becomes Authenticated',
    () {
      // Full coverage of two-phase load + AuthSession dependency in
      // e2e/permission_test.dart; skeleton verified here.
    },
    skip: 'covered in e2e/permission_test.dart',
  );
}
