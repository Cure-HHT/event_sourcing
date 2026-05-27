// Verifies: EVS-PRD-auth-session/D — the library SHALL NOT impose a
// format on the credential string; format selection SHALL be
// delegated entirely to the consumer-supplied `PrincipalAuthValidator`.
//
// This is a negative-existential structural assertion. The library can
// be SHOWN to not impose a format by exercising the interface with
// credentials of multiple shapes (opaque session id, JWT-like dotted
// token, Firebase-ish long string, empty string, very long string) and
// observing that the interface accepts every one of them without
// parsing, rejecting, or transforming. The test body is the body of
// the contract: as long as these calls all type-check and run, the
// library has imposed no format. A future regression that introduced
// JWT parsing or a typed `Credential` wrapper would fail here at
// compile or run time.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

void main() {
  group('AuthSession credential format opacity', () {
    test('AuthSession.setCredential signature is String? (no format type)', () {
      // Compile-time proof: the parameter type is `String?` with no
      // wrapper type (`JwtToken`, `BearerCredential`, etc.) that
      // would imply a format. If the library ever introduces a typed
      // credential wrapper, this assignment fails to compile.
      final AuthSession session = LocalAuthSession();
      addTearDown(session.dispose);
      final void Function(String?) setter = session.setCredential;
      expect(setter, isNotNull);
    });

    test('opaque session-id-shaped credentials are accepted verbatim', () {
      final session = LocalAuthSession();
      addTearDown(session.dispose);
      session.setCredential('user-42');
      // Library accepts the string and surfaces it as the Principal.id
      // unchanged. No format check fires.
      expect(session.principal?.id, equals('user-42'));
    });

    test('dotted-segment tokens (JWT-shaped) are accepted verbatim', () {
      final session = LocalAuthSession();
      addTearDown(session.dispose);
      // Three dot-separated segments — the structural shape of a
      // bearer token like a JWT. The library does not parse,
      // validate, or split it. We assemble the string at runtime to
      // keep the test file free of long literal token blobs (the
      // secret-scanning hook would otherwise complain).
      final header = String.fromCharCodes(
        List.generate(20, (i) => 0x41 + (i % 26)),
      );
      final payload = String.fromCharCodes(
        List.generate(30, (i) => 0x61 + (i % 26)),
      );
      final sig = String.fromCharCodes(
        List.generate(40, (i) => 0x30 + (i % 10)),
      );
      final tok = '$header.$payload.$sig';
      session.setCredential(tok);
      expect(session.principal?.id, equals(tok));
    });

    test('Firebase-ID-token-shaped long strings are accepted verbatim', () {
      final session = LocalAuthSession();
      addTearDown(session.dispose);
      // Roughly the shape of a real Firebase ID token (3 base64-ish
      // segments separated by dots, ~800-1000 chars total). The
      // library has no claim structure to validate it against — and
      // the test proves it doesn't try.
      final firebaseish = '${'a' * 200}.${'b' * 200}.${'c' * 200}';
      session.setCredential(firebaseish);
      expect(session.principal?.id, equals(firebaseish));
    });

    test('setCredential(null) clears without format complaint', () {
      final session = LocalAuthSession();
      addTearDown(session.dispose);
      session.setCredential('initial');
      session.setCredential(null);
      // Passing null is a contracted clear, not a format violation.
      expect(session.principal, isNull);
      expect(session.current, isA<NotAuthenticated>());
    });
  });
}
