import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for the substrate's sealed [Principal] type.
/// UserPrincipal and AnonymousPrincipal are distinguished by a
/// "kind" discriminator. UserPrincipal carries `userId`, `roles`
/// (a set), and `activeRole` (with the invariant
/// `roles.contains(activeRole)`).
///
/// Wire shape:
///   {"kind":"user","userId":"...","roles":["..."],"activeRole":"..."}
///   {"kind":"anonymous"} | {"kind":"anonymous","ipAddress":"..."}
class PrincipalCodec {
  const PrincipalCodec._();

  static Map<String, Object?> encode(Principal p) {
    return switch (p) {
      UserPrincipal() => {
        'kind': 'user',
        'userId': p.userId,
        'roles': p.roles.toList()..sort(),
        'activeRole': p.activeRole,
      },
      AnonymousPrincipal() => {
        'kind': 'anonymous',
        if (p.ipAddress != null) 'ipAddress': p.ipAddress,
      },
    };
  }

  static Principal decode(Map<String, Object?> json) {
    final kind = requireString(json, 'kind');
    switch (kind) {
      case 'user':
        final rolesList = json['roles'];
        if (rolesList is! List) {
          throw FormatException('missing or non-list "roles": $rolesList');
        }
        return UserPrincipal(
          userId: requireString(json, 'userId'),
          roles: rolesList.cast<String>().toSet(),
          activeRole: requireString(json, 'activeRole'),
        );
      case 'anonymous':
        return AnonymousPrincipal(ipAddress: readString(json, 'ipAddress'));
      default:
        throw FormatException('unknown principal kind: $kind');
    }
  }
}
