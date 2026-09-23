# third_party_backend

Third-party `StorageBackend` fixture for
`test/sync/internal_member_fixture_test.dart`, staged as a package of its
own. `ThirdPartyBackend` overrides every contract member through the
barrel alone and marks each override of an internal member `@internal`;
the package must analyze clean. The declarations sit under `lib/src/`,
because `@internal` on a declaration in a public library is itself a
diagnostic. `UnguardedThirdPartyBackend` overrides one internal member
without the annotation, so a call through it from another package is not
reported. A contract change updates `ThirdPartyBackend` in the same change.
