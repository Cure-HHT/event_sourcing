// Implements: EVS-PRD-permissions-as-events (substrate-evaluated containment lookup via TableProjections; fail-closed on missing row)

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/scope_class_registry.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Signature matching `StorageBackend.findViewRowsInTxn`. Passed by the
/// production policy; unit tests provide a fake.
typedef FindRowsInTxn =
    Future<List<Map<String, dynamic>>> Function(
      Txn txn,
      String viewName, {
      Map<String, Object?>? where,
      int? limit,
      int? offset,
    });

/// Walks the containment chain from a [BoundScope]'s class up toward a
/// target class, reading each hop's parent value from the
/// `ContainmentRef.projection`. Returns the resolved ancestor scope or
/// null if any hop misses (fail-closed).
class ContainmentResolver {
  ContainmentResolver({required this.registry, required this.findRowsInTxn});

  final ScopeClassRegistry registry;
  final FindRowsInTxn findRowsInTxn;

  Future<BoundScope?> resolve({
    required Txn txn,
    required BoundScope from,
    required String target,
  }) async {
    if (from.class_ == target) return from;
    if (!registry.isAncestor(target, from.class_)) return null;

    var current = from;
    while (current.class_ != target) {
      final spec = registry.byName(current.class_);
      final ref = spec?.containedIn;
      if (ref == null) return null;
      final rows = await findRowsInTxn(
        txn,
        ref.projection,
        where: {ref.keyColumn: current.value},
        limit: 1,
      );
      if (rows.isEmpty) return null;
      // rows.first (not .single) because limit: 1 is a backend hint, not a
      // contract; a backend returning >1 rows must not crash the authorize
      // stage out of fail-closed.
      final parentValue = rows.first[ref.parentColumn];
      if (parentValue is! String || parentValue.isEmpty) return null;
      current = BoundScope(class_: ref.parentClass, value: parentValue);
    }
    return current;
  }
}
