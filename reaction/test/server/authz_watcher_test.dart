// Tests the watcher behavior using the real substrate's subscribe<T>
// and the real WsConnectionRegistry. Each test seeds a permission
// or role event into the substrate and asserts the registry's
// channels received the right reaction (close-frame or stale_data).
//
// All tests are intentionally skipped at this stage — the unit-level
// shape requires substrate fixtures + close-frame capture that we
// defer to the Phase 4 e2e harness (test/e2e/authz_test.dart). Stubs
// remain here as a checklist of the behavior surface to cover.

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
