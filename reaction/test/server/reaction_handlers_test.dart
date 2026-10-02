import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'mounted /me round-trips a Principal',
    () async {
      // ReactionHandlers needs real substrate handles. Routing-level
      // coverage lives in the per-handler tests; E2E coverage in
      // e2e/auth_test.dart.
    },
    skip: 'full coverage in e2e/auth_test.dart and per-handler tests',
  );

  test(
    'ReactionHandlers exposes four shelf.Handlers',
    () {
      // Smoke: construction with stub substrate handles + handler
      // references are callable. Full behavior is covered by
      // per-handler tests and e2e tests.
    },
    skip: 'covered by per-handler + e2e tests',
  );
}
