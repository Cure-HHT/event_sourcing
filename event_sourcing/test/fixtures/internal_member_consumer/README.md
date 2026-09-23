# internal_member_consumer

Out-of-package consumer fixture for
`test/sync/internal_member_fixture_test.dart`. The test stages this package
and `../third_party_backend/` in a temporary directory, each with a pubspec
naming its dependencies by path, then analyzes both:

- every `calls_*_internal*.dart` file must be reported for
  `invalid_use_of_internal_member`, whether it reaches the member through
  the barrel, a `src/` import, or a third-party backend whose override is
  itself marked internal;
- `calls_transaction_and_close.dart` (the public reads, `transaction` and
  `close`) and `calls_unguarded_third_party.dart` (a call through a
  third-party override that carries no annotation, which the analyzer
  cannot see) must analyze clean.
