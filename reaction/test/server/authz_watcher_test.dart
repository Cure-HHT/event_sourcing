// Tests the watcher behavior using the real substrate's subscribe<T>
// and the real WsConnectionRegistry. Each test seeds a permission
// or role event into the substrate and asserts the registry's
// channels received the right reaction (close-frame or stale_data).
//
// All tests are skipped — full coverage is deferred to the Phase 4
// e2e harness (test/e2e/authz_test.dart). Stubs here serve as a
// checklist of the behavior surface to cover.
//
// Verifies: EVS-DEV-authz-watcher/A/B/C/D/E — force-logout on
//   role_unassigned and permission_revoked, stale_data on role_assigned
//   and permission_granted, containment opt-in via watchContainment,
//   single server-wide substrate subscription. Coverage currently
//   skipped (see note above); the e2e harness exercises the same
//   assertions end-to-end.

import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    "role_unassigned closes the affected user's WS with 4003",
    () async {
      // Seed substrate with a role assignment for alice; register a
      // CaptureChannel for alice on the registry; emit a
      // role_unassigned event; assert closedCode == 4003.
    },
    skip: 'unit pattern; covered in e2e/authz_test.dart (Phase 4)',
  );

  test(
    "role_assigned sends stale_data to the user's connections",
    () async {
      // Seed; emit role_assigned for alice; assert the CaptureChannel
      // received a stale_data envelope with reason: role_assigned.
    },
    skip: 'unit pattern; covered in e2e/authz_test.dart (Phase 4)',
  );

  test(
    'permission_revoked closes WS for all users with that role',
    () async {
      // Seed; emit permission_revoked for a role; assert all users
      // currently holding that activeRole get their WS closed with 4003.
    },
    skip: 'unit pattern; covered in e2e/authz_test.dart (Phase 4)',
  );

  test(
    'permission_granted sends stale_data to all users with that role',
    () async {
      // Seed; emit permission_granted; assert all users with that
      // activeRole receive a stale_data envelope.
    },
    skip: 'unit pattern; covered in e2e/authz_test.dart (Phase 4)',
  );
}
