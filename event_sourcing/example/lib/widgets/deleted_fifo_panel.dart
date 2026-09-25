import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart' show FifoEntry, FinalStatus;
import 'package:event_sourcing_demo/storage_watch.dart';
import 'package:event_sourcing_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

/// Read-only column for a destination the hub deleted. A deletion keeps the
/// destination's delivered, wedged and recovered queue items as its
/// delivery record; this panel lists them from the event store's reader,
/// refreshed as the queue changes.
class DeletedFifoPanel extends StatefulWidget {
  const DeletedFifoPanel({
    required this.destinationId,
    required this.watch,
    super.key,
  });

  final String destinationId;
  final StorageWatch watch;

  @override
  State<DeletedFifoPanel> createState() => _DeletedFifoPanelState();
}

class _DeletedFifoPanelState extends State<DeletedFifoPanel> {
  List<FifoEntry> _rows = const <FifoEntry>[];
  StreamSubscription<List<FifoEntry>>? _sub;

  @override
  void initState() {
    super.initState();
    _sub = widget.watch.queue(widget.destinationId).listen((rows) {
      if (!mounted) return;
      setState(() => _rows = rows);
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  Color _colorOf(FinalStatus? status) => switch (status) {
    FinalStatus.sent => DemoColors.sent,
    FinalStatus.wedged => DemoColors.wedged,
    FinalStatus.tombstoned => DemoColors.pending,
    null => DemoColors.fg,
  };

  @override
  Widget build(BuildContext context) {
    final display = <FifoEntry>[..._rows]
      ..sort((a, b) => b.sequenceInQueue.compareTo(a.sequenceInQueue));
    return Container(
      color: DemoColors.bg,
      padding: const EdgeInsets.all(6),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text(
            '${widget.destinationId} (deleted)',
            style: const TextStyle(
              color: DemoColors.accent,
              fontFamily: 'monospace',
              fontSize: 16,
              fontWeight: FontWeight.bold,
            ),
          ),
          const Text(
            'retained delivery record',
            style: TextStyle(
              color: DemoColors.pending,
              fontFamily: 'monospace',
              fontSize: 11,
            ),
          ),
          Expanded(
            child: ListView.builder(
              itemCount: display.length,
              itemBuilder: (context, i) {
                final row = display[i];
                final status = row.finalStatus?.name ?? 'pending';
                return Text(
                  '#${row.sequenceInQueue} $status '
                  '(${row.eventIds.length} event'
                  '${row.eventIds.length == 1 ? '' : 's'})',
                  key: ValueKey<String>('deleted_row_${row.entryId}'),
                  style: TextStyle(
                    color: _colorOf(row.finalStatus),
                    fontFamily: 'monospace',
                    fontSize: 12,
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}
