// lib/src/permissions/yaml_seed_loader.dart
// Implements: EVS-PRD-permissions-as-events/A
// parses YAML seed
// configuration into a PermissionSeed value; the resulting seed is
// realised as permission_granted events by PermissionSeedApplier, ensuring
// that all initial grants enter the event log.

import 'dart:io';

import 'package:event_sourcing/src/permissions/permission_seed.dart';
import 'package:yaml/yaml.dart';

class YamlSeedLoader {
  PermissionSeed loadFromFile(String path) {
    final yaml = File(path).readAsStringSync();
    return loadFromString(yaml);
  }

  PermissionSeed loadFromString(String yaml) {
    final dynamic doc = loadYaml(yaml);
    if (doc is! YamlMap) {
      throw const FormatException('seed yaml: expected top-level map');
    }
    final dynamic rolesNode = doc['roles'];
    final dynamic grantsNode = doc['grants'];
    if (rolesNode is! YamlList) {
      throw const FormatException('seed yaml: missing or non-list "roles"');
    }
    if (grantsNode is! YamlMap) {
      throw const FormatException('seed yaml: missing or non-map "grants"');
    }
    final roles = rolesNode.cast<String>().toSet();
    final grants = <String, Set<String>>{};
    for (final entry in grantsNode.entries) {
      final role = entry.key as String;
      final perms = entry.value as YamlList;
      grants[role] = perms.cast<String>().toSet();
    }
    return PermissionSeed(roles: roles, grants: grants);
  }
}
