// IMPLEMENTS REQUIREMENTS:
//   entry.
//
// Server-side userId -> Principal resolver. Seed comes from tool/users.yaml at
// boot via UserDirectorySeedApplier; runtime mutations come from
// ProvisionUserAction via UserDirectoryMaterializer. Anonymous for any
// unrecognized or null userId.

import 'package:action_permissions_demo/shared/wire_types.dart';
import 'package:event_sourcing/event_sourcing.dart' show Principal;

class UserDirectory {
  UserDirectory();

  final Map<String, _Entry> _entries = <String, _Entry>{};

  Principal resolve(String? userId) {
    if (userId == null) return const Principal.anonymous();
    final entry = _entries[userId];
    if (entry == null) return const Principal.anonymous();
    return Principal.user(
      userId: userId,
      roles: <String>{entry.role},
      activeRole: entry.role,
    );
  }

  void upsert({
    required String userId,
    required String role,
    required String? activeSite,
  }) {
    _entries[userId] = _Entry(role: role, activeSite: activeSite);
  }

  bool contains(String userId) => _entries.containsKey(userId);

  /// Returns the demo-domain "active site" recorded for [userId], or
  /// null if the user is unknown or has no site. The substrate's
  /// `UserPrincipal` no longer carries an activeSite (CUR-1331), so the
  /// demo sources this directly from its directory record.
  String? siteFor(String userId) => _entries[userId]?.activeSite;

  List<UserDirectoryEntry> listEntries() {
    final ids = _entries.keys.toList()..sort();
    return ids
        .map(
          (id) => UserDirectoryEntry(
            userId: id,
            role: _entries[id]!.role,
            activeSite: _entries[id]!.activeSite,
          ),
        )
        .toList();
  }
}

final class _Entry {
  const _Entry({required this.role, required this.activeSite});

  final String role;
  final String? activeSite;
}
