// test/permissions/test_support/policy_harness.dart
// Test infrastructure for TableBackedAuthorizationPolicy: builds a
// sembast-backed EventStore with role_permission_grants, user_role_scopes,
// and a patient_site_index projection registered; seeds the views by
// appending the corresponding events; constructs a two-class
// ScopeClassRegistry (site top-level, patient contained in site); and
// hands the test a ready-to-call policy plus a Principal-builder helper.

import 'package:event_sourcing/event_sourcing.dart';
// AggregateIdKey and WholePayload are not re-exported from the barrel.
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:sembast/sembast_memory.dart';

/// Test grant: role -> permission-name. Pumped through bootstrap-equivalent
/// permission_granted events.
class Grant {
  const Grant({required this.role, required this.perm});
  final String role;
  final String perm;
}

/// Test assignment: user -> (role, scope). Pumped through role_assigned
/// events with aggregateId derived from the canonical-JSON aggregate-id
/// helper so the user_role_scopes projection keys consistently.
class Assignment {
  const Assignment({
    required this.userId,
    required this.role,
    required this.scope,
  });

  final String userId;
  final String role;
  final ScopeValue scope;
}

/// Projection spec for the patient->site containment lookup used by the
/// ContainmentResolver. The harness emits `patient_site_set` events whose
/// payload carries `{patient_id, site_id}`; the projection keys by
/// patient_id so a single row exists per patient.
const patientSiteIndexSpec = TableProjectionSpec(
  viewName: 'patient_site_index',
  interest: SubscriptionFilter(
    eventTypes: {'patient_site_set', 'patient_site_cleared'},
    aggregateTypes: {'patient_site_assignment'},
  ),
  insertEventTypes: {'patient_site_set'},
  removeEventTypes: {'patient_site_cleared'},
  // Key on aggregateId (== patient_id; see PolicyHarness.create where the
  // appended event uses patient_id as the aggregate-id) so the row layout
  // exposes patient_id and site_id under those exact column names — that
  // matches the ScopeClassRegistry containment ref the harness wires up.
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// Stand-in for a real ScopeProjectionDescriptor; used by the registry's
/// composition-time column validation. The harness only needs to declare
/// the columns the patient_site_index actually produces.
class _PatientSiteIndexDescriptor implements ScopeProjectionDescriptor {
  const _PatientSiteIndexDescriptor();
  @override
  Set<String> get columns => const {'patient_id', 'site_id'};
}

class PolicyHarness {
  PolicyHarness._({
    required this.eventStore,
    required this.backend,
    required this.policy,
  });

  final EventStore eventStore;
  final SembastBackend backend;
  final TableBackedAuthorizationPolicy policy;

  /// Build a harness wired with the three projections and a two-class
  /// scope registry (site top-level; patient contained in site). Seeds
  /// the views by appending [grants], [assignments], and
  /// [patientToSite] containment rows.
  static Future<PolicyHarness> create({
    required List<Grant> grants,
    required List<Assignment> assignments,
    required Map<String, String> patientToSite,
  }) async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'policy-harness-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    final backend = SembastBackend(database: db);

    final typeRegistry = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'role_permission_grant',
          registeredVersion: 1,
          name: 'Role-permission grant',
          isMaterialized: false,
        ),
      )
      ..register(
        const EntryTypeDefinition(
          id: 'user_role_scope',
          registeredVersion: 1,
          name: 'User-role-scope assignment',
          isMaterialized: false,
        ),
      )
      ..register(
        const EntryTypeDefinition(
          id: 'patient_site_assignment',
          registeredVersion: 1,
          name: 'Patient-to-site assignment',
          isMaterialized: false,
        ),
      );

    final projections = ProjectionRegistry()
      ..register(rolePermissionGrantsSpec)
      ..register(userRoleScopesSpec)
      ..register(patientSiteIndexSpec);

    final sourceId = 'policy-harness-${DateTime.now().microsecondsSinceEpoch}';
    final eventStore = await EventStore.open(
      storage: backend,
      entryTypes: typeRegistry,
      source: Source(
        hopId: 'test-server',
        identifier: sourceId,
        softwareVersion: 'event_sourcing_test@0.0.0',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: projections,
    );

    // Emit permission_granted events for each grant.
    for (final g in grants) {
      await eventStore.append(
        entryType: 'role_permission_grant',
        aggregateType: 'role_permission_grant',
        aggregateId: '${g.role}:${g.perm}',
        eventType: 'permission_granted',
        data: PermissionGrantedPayload(
          role: g.role,
          permissionName: g.perm,
        ).toJson(),
        initiator: const AutomationInitiator(service: 'policy_harness'),
      );
    }

    // Emit role_assigned events for each assignment.
    for (final a in assignments) {
      await eventStore.append(
        entryType: 'user_role_scope',
        aggregateType: 'user_role_scope',
        aggregateId: computeRoleAssignmentAggregateId(
          userId: a.userId,
          role: a.role,
          scope: a.scope,
        ),
        eventType: 'role_assigned',
        data: RoleAssignedPayload(
          userId: a.userId,
          role: a.role,
          scope: a.scope,
        ).toJson(),
        initiator: const AutomationInitiator(service: 'policy_harness'),
      );
    }

    // Emit patient_site_set events for each containment row.
    for (final entry in patientToSite.entries) {
      await eventStore.append(
        entryType: 'patient_site_assignment',
        aggregateType: 'patient_site_assignment',
        aggregateId: entry.key,
        eventType: 'patient_site_set',
        data: <String, Object?>{
          'patient_id': entry.key,
          'site_id': entry.value,
        },
        initiator: const AutomationInitiator(service: 'policy_harness'),
      );
    }

    // Build the scope-class registry. patient_site_index columns are
    // declared via the local descriptor; the other projections used by
    // the policy (role_permission_grants, user_role_scopes) are not
    // referenced from any containment, so they need no descriptor.
    final scopeClassRegistry = ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'site'),
        ScopeClassSpec(
          name: 'patient',
          containedIn: ContainmentReference(
            parentClass: 'site',
            projection: 'patient_site_index',
            keyColumn: 'patient_id',
            parentColumn: 'site_id',
          ),
        ),
      ],
      projectionLookup: (name) => name == 'patient_site_index'
          ? const _PatientSiteIndexDescriptor()
          : null,
    );

    final policy = TableBackedAuthorizationPolicy(
      backend: backend,
      scopeClassRegistry: scopeClassRegistry,
      transactionProvider: <T>(fn) => backend.transaction<T>(fn),
    );

    return PolicyHarness._(
      eventStore: eventStore,
      backend: backend,
      policy: policy,
    );
  }

  /// Coherent UserPrincipal for [userId] with [activeRole] (also included
  /// in the roles set so the activeRole-in-roles invariant holds).
  UserPrincipal user(String userId, String activeRole) {
    return UserPrincipal(
      userId: userId,
      roles: {activeRole},
      activeRole: activeRole,
    );
  }

  Future<void> close() => eventStore.close();
}
