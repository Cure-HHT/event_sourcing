// The one adapter from a [StorageReader]'s converging-aware view read to
// the plain [FindRowsInTxn] shape [ContainmentResolver] and
// [ScopeDescendantExpander] both take. It is the sole place either walker's
// callback checks a read's reported convergence state, so no caller of
// either can forget the check.
// Implements: EVS-DEV-converging-view-reads/H
// every library operation that decides from a view's rows -- the
//   ContainmentResolver's per-hop reads and the ScopeDescendantExpander's
//   per-hop reads among them -- checks the read's reported state and
//   throws ViewConvergingRefusal naming the view rather than deciding from
//   an unsettled read.

import 'package:event_sourcing/src/permissions/containment_resolver.dart'
    show ContainmentResolver, FindRowsInTxn;
import 'package:event_sourcing/src/permissions/scope_descendant_expander.dart'
    show ScopeDescendantExpander;
import 'package:event_sourcing/src/projections/view_read.dart'
    show ViewConvergenceState, ViewConvergingRefusal;
import 'package:event_sourcing/src/storage/storage_reader.dart';

/// A [FindRowsInTxn] reading a view through [reader]: the rows of a
/// current view, or a thrown [ViewConvergingRefusal] naming the view while
/// its copy converges for this instance. Pass the result to
/// [ContainmentResolver] or [ScopeDescendantExpander] so both walkers
/// share one currency check.
FindRowsInTxn currentViewRows(StorageReader reader) {
  return (txn, viewName, {where, limit, offset}) async {
    final read = await reader.findViewRowsInTxn(
      txn,
      viewName,
      where: where,
      limit: limit,
      offset: offset,
    );
    if (read.state == ViewConvergenceState.converging) {
      throw ViewConvergingRefusal(viewName);
    }
    return read.rows;
  };
}
