import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

void main() {
  test('round-trips minimal EffectiveAuthorization (no scope assignments)', () {
    final original = EffectiveAuthorization(
      activeRole: 'install',
      rolePermissions: {const Permission('greet.send')},
      scopeAssignments: const [],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips with scope assignments (multiple bound scopes)', () {
    final original = EffectiveAuthorization(
      activeRole: 'StudyCoordinator',
      rolePermissions: {
        const Permission('patient.view', scopeClass: 'patient'),
        const Permission('patient.edit', scopeClass: 'patient'),
      },
      scopeAssignments: const [
        ScopeAssignment(
          scope: BoundScope(class_: 'site', value: 'A'),
        ),
        ScopeAssignment(
          scope: BoundScope(class_: 'site', value: 'B'),
        ),
      ],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips with total wildcard scope', () {
    final original = EffectiveAuthorization(
      activeRole: 'Admin',
      rolePermissions: {const Permission('admin.all')},
      scopeAssignments: const [ScopeAssignment(scope: TotalWildcardScope())],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded, original);
    expect(decoded.scopeAssignments.first.scope, isA<TotalWildcardScope>());
  });

  test('round-trips with value-wildcard scope', () {
    final original = EffectiveAuthorization(
      activeRole: 'SiteAdmin',
      rolePermissions: {const Permission('site.view', scopeClass: 'site')},
      scopeAssignments: const [
        ScopeAssignment(scope: ValueWildcardScope(class_: 'site')),
      ],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded, original);
    expect(decoded.scopeAssignments.first.scope, isA<ValueWildcardScope>());
  });

  test('round-trips EffectiveAuthorization.empty (canonical anonymous)', () {
    const original = EffectiveAuthorization.empty;
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded, EffectiveAuthorization.empty);
    expect(decoded.activeRole, '');
    expect(decoded.rolePermissions, isEmpty);
    expect(decoded.scopeAssignments, isEmpty);
  });
}
