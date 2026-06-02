// Implements: EVS-PRD-scoped-permissions/F — read-path hierarchy expansion:
//   enumerate the descendant-class scope values reachable from an ancestor
//   assignment by reading ContainmentReference projections downward.
// Implements: EVS-PRD-scoped-permissions/G — fail-closed on missing
//   containment rows (a missing/malformed row contributes nothing).
// Implements: EVS-DEV-scope-descendant-expander — downward chain-walk:
//   A identity on equal class; B empty on non-ancestor target; C per-hop
//   inverse projection read (where parentColumn = value -> read keyColumn);
//   D fail-closed skip on missing/malformed row; E breadth-first multi-hop
//   fan-out with set union.

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/containment_resolver.dart'
    show
        ContainmentResolver,
        FindRowsInTxn; // reuse the resolver's callback signature
import 'package:event_sourcing/src/permissions/scope_class_registry.dart';
import 'package:event_sourcing/src/storage/transaction.dart';

/// Walks the containment graph DOWNWARD from an ancestor [BoundScope] to a
/// descendant class, enumerating the descendant scope values reachable
/// through the `ContainmentReference` projections.
///
/// This is the inverse of [ContainmentResolver]: the resolver answers
/// "what is P-42's site?" (child -> parent, used by the action path); the
/// expander answers "which participants are at site-A?" (parent -> all
/// children, used by the read path to narrow a subscription).
///
/// Fail-closed: a missing or malformed index row contributes nothing
/// (never widens). A non-ancestor assignment returns the empty set.
class ScopeDescendantExpander {
  ScopeDescendantExpander({
    required this.registry,
    required this.findRowsInTxn,
  });

  final ScopeClassRegistry registry;
  final FindRowsInTxn findRowsInTxn;

  Future<Set<String>> expand({
    required Transaction txn,
    required BoundScope assignment,
    required String targetClass,
  }) async {
    // A: identity — the assignment is already at the target class.
    if (assignment.class_ == targetClass) return {assignment.value};
    // B: the assignment's class must be an ancestor of the target.
    if (!registry.isAncestor(assignment.class_, targetClass)) {
      return <String>{};
    }

    // Build a partial ancestor chain from targetClass up to (and including)
    // assignment.class_, then reverse it to get top-down descent order.
    // ancestorChain yields child-first, so iterate and stop at the
    // assignment's class. The assignment's class is NOT a hop to resolve —
    // the frontier starts there, so it is dropped from childClasses below.
    final chain = registry
        .ancestorChain(targetClass)
        .toList(); // [target, ..., assignmentClass]
    final descendOrder = <String>[];
    for (final spec in chain) {
      descendOrder.add(spec.name);
      if (spec.name == assignment.class_) break;
    }
    // descendOrder == [target, ..., assignmentClass]; reverse to top-down,
    // then drop the assignment's class (the frontier starts there).
    final topDown = descendOrder.reversed
        .toList(); // [assignmentClass, ..., target]
    final childClasses = topDown.sublist(1); // classes to resolve into

    var frontier = <String>{assignment.value};
    for (final childClass in childClasses) {
      final spec = registry.byName(childClass);
      final ref = spec?.containedIn;
      if (ref == null) return <String>{}; // chain broke; fail-closed
      final next = <String>{};
      for (final parentValue in frontier) {
        final rows = await findRowsInTxn(
          txn,
          ref.projection,
          where: {ref.parentColumn: parentValue},
        );
        for (final row in rows) {
          final key = row[ref.keyColumn];
          if (key is String && key.isNotEmpty) next.add(key);
        }
      }
      frontier = next;
    }
    return frontier;
  }
}
