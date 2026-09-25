# internal_member_consumer

Out-of-package consumer fixture for
`test/sync/internal_member_fixture_test.dart`. The test stages this package
and `../third_party_backend/` in a temporary directory, each with a pubspec
naming its dependencies by path, then analyzes both:

- every `calls_*_internal*.dart` file must be reported for
  `invalid_use_of_internal_member`, whether it reaches the member through
  the barrel, a `src/` import, or a third-party backend whose override is
  itself marked internal;
- `calls_library_private_members.dart` must be reported only for undefined
  members: the writing, publishing and handle-yielding members of the
  types the library hands out are private to their Dart library, so a
  consumer cannot name them;
- `calls_test_only_open.dart` must be reported for
  `invalid_use_of_visible_for_testing_member`: `EventStore.openForTest`
  is test-only;
- `calls_transaction_and_close.dart` (the public reads, `transaction`,
  `close` and the `LibVersion` constants) and `calls_unguarded_third_party.dart` (a call through a
  third-party override that carries no annotation, which the analyzer
  cannot see) must analyze clean.
