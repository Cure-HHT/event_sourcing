import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/principal_codec.dart';

void main() {
  test('round-trips UserPrincipal', () {
    final original = UserPrincipal(
      userId: 'u-123',
      roles: const {'study-coordinator', 'supervisor'},
      activeRole: 'study-coordinator',
    );
    final json = PrincipalCodec.encode(original);
    final decoded = PrincipalCodec.decode(json) as UserPrincipal;
    expect(decoded.userId, 'u-123');
    expect(decoded.roles, original.roles);
    expect(decoded.activeRole, 'study-coordinator');
  });

  test('round-trips AnonymousPrincipal with ipAddress', () {
    const original = AnonymousPrincipal(ipAddress: '198.51.100.7');
    final json = PrincipalCodec.encode(original);
    final decoded = PrincipalCodec.decode(json) as AnonymousPrincipal;
    expect(decoded.ipAddress, '198.51.100.7');
  });

  test('round-trips AnonymousPrincipal without ipAddress', () {
    const original = AnonymousPrincipal();
    final json = PrincipalCodec.encode(original);
    final decoded = PrincipalCodec.decode(json) as AnonymousPrincipal;
    expect(decoded.ipAddress, isNull);
  });

  test('decode rejects unknown principal kind', () {
    expect(
      () => PrincipalCodec.decode({'kind': 'alien'}),
      throwsA(isA<FormatException>()),
    );
  });
}
