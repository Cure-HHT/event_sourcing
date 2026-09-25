import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/storage_watch.dart';
import 'package:event_sourcing_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

/// The WEDGED column: the rows of the library's default destination-wedges
/// view, one per wedged destination, and the latest delivery events the
/// view is folded from.
///
/// A row whose `database_id` is the pane's own database identity is one of
/// the pane's own wedges and carries a "Recover" button, which recovers the
/// wedged head (`tombstoneAndRefill`). Any other row is a peer's wedge,
/// folded from a wedge event the peer forwarded: it is labelled with its
/// origin database and carries no action, because a registry operation acts
/// only on its own database, and this pane's destination of the same id is
/// a different queue.
class WedgesPanel extends StatefulWidget {
  const WedgesPanel({
    required this.watch,
    required this.databaseId,
    required this.appState,
    super.key,
  });

  final StorageWatch watch;

  /// The pane's database identity (`EventStore.databaseId`).
  final String databaseId;
  final AppState appState;

  @override
  State<WedgesPanel> createState() => _WedgesPanelState();
}

/// The delivery events the panel lists, by entry type, with a short label.
const Map<String, String> _deliveryEvents = <String, String>{
  kDestinationWedgedEntryType: 'wedged',
  kDestinationHaltRequestedEntryType: 'halt requested',
  kDestinationHaltCancelledEntryType: 'halt cancelled',
  kDestinationWedgeRecoveredEntryType: 'recovered',
  kDestinationDeletedEntryType: 'deleted',
};

/// The message of a registry refusal, for display.
String refusalText(Object e) => switch (e) {
  StateError(:final message) => message,
  ArgumentError(:final message) => '$message',
  _ => '$e',
};

class _WedgesPanelState extends State<WedgesPanel> {
  StreamSubscription<StoredEvent>? _eventsSub;
  List<Map<String, Object?>> _rows = const <Map<String, Object?>>[];
  List<StoredEvent> _deliveryLog = const <StoredEvent>[];
  String? _banner;

  /// Counts refreshes started, so a read that completes after a later one
  /// does not replace the later, fresher rows.
  int _refreshes = 0;

  @override
  void initState() {
    super.initState();
    _eventsSub = widget.watch.events().listen((_) {
      if (!mounted) return;
      unawaited(_refresh());
    });
    unawaited(_refresh());
  }

  @override
  void dispose() {
    _eventsSub?.cancel();
    super.dispose();
  }

  Future<void> _refresh() async {
    final refresh = ++_refreshes;
    try {
      final rows = await widget.watch.reader.findViewRows(
        defaultDestinationWedgesSpec.viewName,
      );
      final log = <StoredEvent>[
        for (final type in _deliveryEvents.keys)
          ...await widget.watch.reader.findAllEvents(entryType: type),
      ]..sort((a, b) => b.sequenceNumber.compareTo(a.sequenceNumber));
      if (!mounted || refresh != _refreshes) return;
      setState(() {
        _rows = rows;
        _deliveryLog = log.take(8).toList();
      });
    } catch (_) {
      // Non-fatal: the next event refreshes again.
    }
  }

  Future<void> _recover(String id, String rowId) async {
    try {
      await widget.appState.recover(id, rowId);
      _show('recovered $id');
    } catch (e) {
      _show('recovery refused: ${refusalText(e)}');
    }
  }

  void _show(String message) {
    if (!mounted) return;
    setState(() => _banner = message);
  }

  static String _initiatorLabel(Object? initiator) {
    if (initiator is Map) {
      return '${initiator['user_id'] ?? initiator['service'] ?? initiator}';
    }
    return initiator == null ? '-' : '$initiator';
  }

  static String _short(String id) => id.length <= 8 ? id : id.substring(0, 8);

  @override
  Widget build(BuildContext context) {
    final local = <Map<String, Object?>>[];
    final peers = <Map<String, Object?>>[];
    for (final row in _rows) {
      (row['database_id'] == widget.databaseId ? local : peers).add(row);
    }
    return Container(
      decoration: BoxDecoration(color: DemoColors.bg, border: demoBorder),
      padding: const EdgeInsets.all(8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          const Text('WEDGED', style: DemoText.header),
          if (_banner != null)
            Container(
              padding: const EdgeInsets.all(4),
              color: DemoColors.accent,
              child: Text(
                _banner!,
                style: const TextStyle(color: DemoColors.bg, fontSize: 12),
              ),
            ),
          Expanded(
            child: ListView(
              children: <Widget>[
                if (_rows.isEmpty) const Text('none', style: _small),
                for (final row in local) _localRow(row),
                for (final row in peers) _peerRow(row),
                const SizedBox(height: 12),
                const Text('delivery events', style: _smallAccent),
                for (final e in _deliveryLog)
                  Text(
                    '#${e.sequenceNumber} ${e.data['id']}: '
                    '${_deliveryEvents[e.entryType]}'
                    '${e.data['cause'] != null ? ' (${e.data['cause']})' : ''}',
                    style: _small,
                  ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  static const TextStyle _small = TextStyle(
    color: DemoColors.fg,
    fontFamily: 'monospace',
    fontSize: 12,
  );
  static const TextStyle _smallAccent = TextStyle(
    color: DemoColors.accent,
    fontFamily: 'monospace',
    fontSize: 12,
  );

  String _describe(Map<String, Object?> row) {
    final attempts = row['attempt_count'];
    final by = row['halt_requested_by'];
    return '${row['id']}: ${row['cause']}'
        '${attempts == null ? '' : ', $attempts attempts'}'
        '${by == null ? '' : ', halt by ${_initiatorLabel(by)}'}';
  }

  Widget _localRow(Map<String, Object?> row) {
    final id = row['id']! as String;
    final rowId = row['row_id']! as String;
    return Row(
      children: <Widget>[
        Expanded(
          child: Text(
            _describe(row),
            style: const TextStyle(
              color: DemoColors.wedged,
              fontFamily: 'monospace',
              fontSize: 12,
            ),
          ),
        ),
        TextButton(
          onPressed: () => _recover(id, rowId),
          child: const Text(
            'Recover',
            style: TextStyle(color: DemoColors.accent, fontSize: 12),
          ),
        ),
      ],
    );
  }

  Widget _peerRow(Map<String, Object?> row) => Text(
    '${_describe(row)} -- peer, from database '
    '${_short('${row['database_id']}')}',
    style: const TextStyle(
      color: DemoColors.pending,
      fontFamily: 'monospace',
      fontSize: 12,
    ),
  );
}
