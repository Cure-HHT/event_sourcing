import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_datastore_demo/app_state.dart';
import 'package:event_sourcing_datastore_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

// Validated by: JNY-01 materialized lifecycle; JNY-06 rebuild idempotence.
class MaterializedPanel extends StatefulWidget {
  const MaterializedPanel({
    required this.backend,
    required this.appState,
    super.key,
  });

  final StorageBackend backend;
  final AppState appState;

  @override
  State<MaterializedPanel> createState() => _MaterializedPanelState();
}

/// Raw row shape written by AggregateProjectionSpec into the notes view.
/// Fields are whatever demo_note event.data contains (merged via
/// AggregateFold's deep-merge) plus the metadata stamp.
class _ViewRow {
  const _ViewRow({
    required this.aggregateId,
    required this.updatedAt,
    required this.title,
  });

  final String aggregateId;
  final DateTime updatedAt;

  /// Note title extracted from the `answers.title` field written by
  /// the demo's Start / Complete action via `_collectAnswers`. Empty
  /// when the operator left the title field blank.
  final String title;
}

class _MaterializedPanelState extends State<MaterializedPanel> {
  List<_ViewRow> _rows = const <_ViewRow>[];
  Timer? _poll;

  @override
  void initState() {
    super.initState();
    widget.appState.addListener(_onAppState);
    _poll = Timer.periodic(
      const Duration(milliseconds: 500),
      (_) => _refresh(),
    );
    _refresh();
  }

  @override
  void dispose() {
    _poll?.cancel();
    widget.appState.removeListener(_onAppState);
    super.dispose();
  }

  void _onAppState() {
    if (!mounted) return;
    setState(() {});
  }

  Future<void> _refresh() async {
    try {
      // AggregateFold stamps 'aggregateId' and 'updatedAt' into every view row.
      // Tombstone events delete the row, so presence means the aggregate is live.
      // The demo_note action also deep-merges event.data which carries
      // answers.title — extracted here so the panel can show meaningful labels.
      final rawRows = await widget.backend.findViewRows('notes');
      final rows = <_ViewRow>[];
      for (final raw in rawRows) {
        final aggregateId = raw['aggregateId'] as String? ?? '';
        final updatedAtStr = raw['updatedAt'] as String?;
        final updatedAt = updatedAtStr != null
            ? DateTime.tryParse(updatedAtStr) ?? DateTime(0)
            : DateTime(0);
        // Dig out title from the answers map merged in by AggregateFold.
        final answers = raw['answers'] as Map<Object?, Object?>?;
        final title = answers?['title'] as String? ?? '';
        rows.add(
          _ViewRow(
            aggregateId: aggregateId,
            updatedAt: updatedAt,
            title: title,
          ),
        );
      }
      if (!mounted) return;
      setState(() {
        _rows = rows;
      });
    } catch (_) {
      // Transient read failure — next poll retries. Non-fatal for demo.
    }
  }

  @override
  Widget build(BuildContext context) {
    // Most-recent first: sort descending by updatedAt.
    final sorted = <_ViewRow>[..._rows]
      ..sort((a, b) => b.updatedAt.compareTo(a.updatedAt));
    return Container(
      decoration: BoxDecoration(color: DemoColors.bg, border: demoBorder),
      padding: const EdgeInsets.all(8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          const Text('MATERIALIZED', style: DemoText.header),
          const SizedBox(height: 8),
          Expanded(
            child: ListView.builder(
              itemCount: sorted.length,
              itemBuilder: (context, i) => _MaterializedRow(
                row: sorted[i],
                selected:
                    widget.appState.selectedAggregateId ==
                    sorted[i].aggregateId,
                onTap: () =>
                    widget.appState.selectAggregate(sorted[i].aggregateId),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _MaterializedRow extends StatelessWidget {
  const _MaterializedRow({
    required this.row,
    required this.selected,
    required this.onTap,
  });

  final _ViewRow row;
  final bool selected;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context) {
    // UUIDv7 prefix is a timestamp, so entries minted close in time share
    // leading bytes. Tail (rand_b) is where the visual entropy lives.
    final short = row.aggregateId.length >= 8
        ? row.aggregateId.substring(row.aggregateId.length - 8)
        : row.aggregateId;
    // Show title when present; fall back to the UUID tail so the row is
    // never blank. Overflow ellipsis prevents wrapping in the narrow column.
    final label = row.title.isNotEmpty
        ? '${row.title}  (…$short)'
        : 'agg-$short';
    return InkWell(
      onTap: onTap,
      child: Container(
        decoration: BoxDecoration(
          color: selected ? DemoColors.selected : DemoColors.bg,
          border: selected ? demoSelectedBorder : null,
        ),
        padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
        child: Text(
          label,
          style: DemoText.body,
          overflow: TextOverflow.ellipsis,
        ),
      ),
    );
  }
}
