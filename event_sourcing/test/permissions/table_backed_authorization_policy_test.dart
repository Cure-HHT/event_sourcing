// test/permissions/table_backed_authorization_policy_test.dart
// Verifies: EVS-PRD-permissions-as-events/B — TableBackedAuthorizationPolicy
//   evaluates authorization decisions solely from the event-derived
//   role_permission_grants, user_role_scopes, and containment projections.
//   The match algorithm covers equality, value-wildcard, total-wildcard,
//   and hierarchy containment with fail-closed semantics on missing rows.
// Verifies: EVS-PRD-action-dispatch/B — Allow/Deny decisions surfaced to
//   the dispatcher's authorize stage.
// Verifies: EVS-PRD-scoped-permissions/D/F/G — projection-only evaluation;
//   union-of-assignments match across equality / wildcard / containment;
//   fail-closed propagation through missing containment rows.
// Verifies: EVS-DEV-scoped-permissions-match-algorithm/A/B/C/D/E/F — full
//   match-algorithm coverage: notGranted on missing role grant; xor invariant
//   denial; unscoped shortcut; bound / value-wildcard / total-wildcard /
//   containment match cases; fail-closed propagation; anonymous-principal
//   denial.
// Verifies: EVS-DEV-effective-permissions-shape/A/B/C/D — effectivePermissionsFor
//   returns active role + permissions + scope assignments; returns
//   EffectiveAuthorization.empty for non-user principals.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_support/policy_harness.dart';

void main() {
  group('TableBackedAuthorizationPolicy match algorithm', () {
    test('bound site assignment + patient-scoped permission via containment '
        '-> Allow when patient is at the assigned site', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: {'P-42': 'A'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('bound site assignment but patient is at a different site '
        '-> Deny(notGranted)', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: {'P-42': 'B'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(
        decision,
        isA<Deny>().having((d) => d.reason, 'reason', DenyReason.notGranted),
      );
    });

    test(
      'TotalWildcardScope assignment -> Allow on any scoped permission',
      () async {
        final h = await PolicyHarness.create(
          grants: [const Grant(role: 'SUP', perm: 'patient.view')],
          assignments: [
            const Assignment(
              userId: 'U2',
              role: 'SUP',
              scope: TotalWildcardScope(),
            ),
          ],
          patientToSite: {'P-42': 'A'},
        );
        addTearDown(h.close);
        final decision = await h.policy.isPermitted(
          h.user('U2', 'SUP'),
          const Permission('patient.view', scopeClass: 'patient'),
          const BoundScope(class_: 'patient', value: 'P-42'),
        );
        expect(decision, isA<Allow>());
      },
    );

    test('ValueWildcardScope(class=site) assignment -> Allow for any patient '
        'via containment (any site covers any patient at any site)', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U3',
            role: 'SC',
            scope: ValueWildcardScope(class_: 'site'),
          ),
        ],
        patientToSite: {'P-42': 'B'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U3', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('patient-scoped assignment cannot cover site-scoped permission '
        '(narrower than) -> Deny', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'site.read')],
        assignments: [
          const Assignment(
            userId: 'U4',
            role: 'SC',
            scope: BoundScope(class_: 'patient', value: 'P-42'),
          ),
        ],
        patientToSite: {'P-42': 'A'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U4', 'SC'),
        const Permission('site.read', scopeClass: 'site'),
        const BoundScope(class_: 'site', value: 'A'),
      );
      expect(decision, isA<Deny>());
    });

    test('containment lookup miss -> fail-closed Deny(notGranted)', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: const <String, String>{}, // no row for P-42
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(
        decision,
        isA<Deny>().having((d) => d.reason, 'reason', DenyReason.notGranted),
      );
    });

    test('union within active role: U1 has SC at site A AND site B; '
        'P-42 is at B -> Allow', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'B'),
          ),
        ],
        patientToSite: {'P-42': 'B'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('active role filter: U1 holds SC AND SUP, but activeRole=SC; '
        'SUP perms are ignored', () async {
      final h = await PolicyHarness.create(
        grants: [
          const Grant(role: 'SC', perm: 'patient.view'),
          const Grant(role: 'SUP', perm: 'patient.edit'),
        ],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
          const Assignment(
            userId: 'U1',
            role: 'SUP',
            scope: TotalWildcardScope(),
          ),
        ],
        patientToSite: {'P-42': 'A'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(
        decision,
        isA<Deny>().having((d) => d.reason, 'reason', DenyReason.notGranted),
      );
    });

    test('unscoped permission with role grant -> Allow', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'report.generate')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('report.generate'),
        null,
      );
      expect(decision, isA<Allow>());
    });

    // Substrate-verified role-membership gate (closed-under-events trust
    // model): even when the role grants the permission, an unscoped
    // permission check must Deny if the principal holds no
    // user_role_scopes assignment for the claimed activeRole. The
    // Principal's `roles`/`activeRole` fields are user-supplied
    // requests, not substrate-trusted assertions.
    test('unscoped permission with role grant but principal holds no '
        'user_role_scopes row for activeRole -> Deny(notGranted)', () async {
      final h = await PolicyHarness.create(
        // SC has the unscoped report.generate permission ...
        grants: [const Grant(role: 'SC', perm: 'report.generate')],
        // ... but U1 has NO user_role_scopes assignment for SC.
        // (A different user U2 holds SC, to keep the projection
        // non-empty and prove the where-clause is what filters U1
        // out.)
        assignments: [
          const Assignment(
            userId: 'U2',
            role: 'SC',
            scope: TotalWildcardScope(),
          ),
        ],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('report.generate'),
        null,
      );
      expect(
        decision,
        isA<Deny>().having(
          (Deny d) => d.reason,
          'reason',
          DenyReason.notGranted,
        ),
      );
    });

    // Same gate for the effectivePermissionsFor surface: a Principal
    // claiming an activeRole they don't hold sees empty permissions,
    // not the role's grants leaking through the unverified claim.
    test('effectivePermissionsFor returns empty for principal holding no '
        'user_role_scopes row under claimed activeRole', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'report.generate')],
        assignments: [
          // U2 holds SC; U1 does not.
          const Assignment(
            userId: 'U2',
            role: 'SC',
            scope: TotalWildcardScope(),
          ),
        ],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final eff = await h.policy.effectivePermissionsFor(h.user('U1', 'SC'));
      expect(eff, equals(EffectiveAuthorization.empty));
    });

    test('unscoped permission without role grant -> Deny', () async {
      final h = await PolicyHarness.create(
        grants: const <Grant>[],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('report.generate'),
        null,
      );
      expect(decision, isA<Deny>());
    });

    // Defense-in-depth: the dispatcher validates scope shape/class
    // before calling isPermitted, but a direct caller (test, gating
    // helper, etc.) might pass mismatched values. The policy must
    // fail-closed with scopeUnresolvable rather than silently grant
    // or evaluate against stale assignments.

    test('direct call with scoped permission + non-BoundScope scope value '
        '-> Deny(scopeUnresolvable)', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: const <String, String>{'P-42': 'A'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const TotalWildcardScope(),
      );
      expect(
        decision,
        isA<Deny>().having(
          (d) => d.reason,
          'reason',
          DenyReason.scopeUnresolvable,
        ),
      );
    });

    test('direct call with BoundScope whose class disagrees with '
        'permission.scopeClass -> Deny(scopeUnresolvable)', () async {
      final h = await PolicyHarness.create(
        grants: [const Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
        ],
        patientToSite: const <String, String>{'P-42': 'A'},
      );
      addTearDown(h.close);
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'site', value: 'A'),
      );
      expect(
        decision,
        isA<Deny>().having(
          (d) => d.reason,
          'reason',
          DenyReason.scopeUnresolvable,
        ),
      );
    });
  });

  group('TableBackedAuthorizationPolicy.effectivePermissionsFor', () {
    test('returns active role permissions + user assignments for it', () async {
      final h = await PolicyHarness.create(
        grants: [
          const Grant(role: 'SC', perm: 'patient.edit'),
          const Grant(role: 'SC', perm: 'patient.view'),
          const Grant(role: 'OTHER', perm: 'other.thing'),
        ],
        assignments: [
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ),
          const Assignment(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'B'),
          ),
          const Assignment(
            userId: 'U1',
            role: 'OTHER',
            scope: TotalWildcardScope(),
          ),
        ],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final eff = await h.policy.effectivePermissionsFor(h.user('U1', 'SC'));
      expect(eff.activeRole, 'SC');
      expect(eff.rolePermissions.map((p) => p.name).toSet(), <String>{
        'patient.edit',
        'patient.view',
      });
      expect(eff.scopeAssignments.map((a) => a.scope).toSet(), <ScopeValue>{
        const BoundScope(class_: 'site', value: 'A'),
        const BoundScope(class_: 'site', value: 'B'),
      });
    });

    test('returns empty for AnonymousPrincipal', () async {
      final h = await PolicyHarness.create(
        grants: const <Grant>[],
        assignments: const <Assignment>[],
        patientToSite: const <String, String>{},
      );
      addTearDown(h.close);
      final eff = await h.policy.effectivePermissionsFor(
        const AnonymousPrincipal(),
      );
      expect(eff, equals(EffectiveAuthorization.empty));
    });
  });
}
