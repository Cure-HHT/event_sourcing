import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

class DetailPanel extends StatefulWidget {
  const DetailPanel({
    required this.backend,
    required this.databaseId,
    required this.appState,
    required this.policyNotifier,
    super.key,
  });

  final SembastBackend backend;

  /// The pane's database identity (`EventStore.databaseId`): rows of the
  /// default destination-wedges view whose `database_id` is this identity
  /// are this pane's own wedges; every other row is a peer's.
  final String databaseId;
  final AppState appState;
  final ValueNotifier<SyncPolicy> policyNotifier;

  @override
  State<DetailPanel> createState() => _DetailPanelState();
}

class _DetailPanelState extends State<DetailPanel> {
  StreamSubscription<StoredEvent>? _eventsSub;
  String? _summary;

  @override
  void initState() {
    super.initState();
    widget.appState.addListener(_onAppState);
    widget.policyNotifier.addListener(_onChange);
    _eventsSub = widget.backend.watchEvents().listen((_) {
      if (!mounted) return;
      _refresh();
    });
    _refresh();
  }

  @override
  void dispose() {
    _eventsSub?.cancel();
    widget.appState.removeListener(_onAppState);
    widget.policyNotifier.removeListener(_onChange);
    super.dispose();
  }

  void _onChange() {
    if (!mounted) return;
    setState(() {});
  }

  void _onAppState() {
    _onChange();
    if (!mounted) return;
    // A registry operation, or a restarted delivery cycle, changes the
    // delivery status without always appending an event.
    unawaited(_refresh());
  }

  Future<void> _refresh() async {
    try {
      final events = await widget.backend.findAllEvents(limit: 100000);
      final anyWedged = await widget.backend.hasFifoWedged();
      // The pane's own queues, read directly.
      final wedged = await widget.backend.wedgedFifos();
      // The library's default destination-wedges view, folded from the
      // wedge, recovery and deletion events in the log. Its local rows name
      // the same wedged heads as wedgedFifos() (each read is its own
      // snapshot, so the two can differ while the drainer runs between
      // them); its peer rows come from wedge events another pane forwarded,
      // which no read of this pane's queues shows.
      final viewRows = await widget.backend.findViewRows(
        defaultDestinationWedgesSpec.viewName,
      );
      final local = <String>[];
      final peers = <String>[];
      for (final row in viewRows) {
        final id = row['id'] as String? ?? '?';
        final cause = row['cause'] as String? ?? '?';
        final origin = row['database_id'] as String? ?? '?';
        if (origin == widget.databaseId) {
          local.add('$id ($cause)');
        } else {
          peers.add('$id ($cause) from database ${_short(origin)}');
        }
      }
      final aggCount = events.map((e) => e.aggregateId).toSet().length;
      // The pane's delivery cycle, and the database's persisted delivery
      // status, which any process can read, draining or not.
      final cycle = widget.appState.cycle;
      final status = await widget.appState.readDeliveryStatus();
      final drainer = status.drainer;
      final heartbeat = status.heartbeat;
      final unserved = <String>[
        for (final e
            in (cycle?.unserved ?? const <String, UnservedReason>{}).entries)
          '${e.key} (${e.value.wire})',
      ];
      final drainerLine = drainer == null
          ? 'none declared'
          : 'epoch ${drainer.epoch}, '
                'version ${drainer.configurationVersion ?? '-'}';
      final heartbeatLine = heartbeat == null
          ? 'none'
          : 'epoch ${heartbeat.epoch} pass ${heartbeat.pass}';
      final text = <String>[
        'events:     ${events.length}',
        'aggregates: $aggCount',
        'any wedged: $anyWedged',
        if (wedged.isNotEmpty)
          'wedged dst: ${wedged.map((s) => s.destinationId).join(", ")}',
        'wedges view (this database): ${_listOrNone(local)}',
        'wedges view (peers):         ${_listOrNone(peers)}',
        '',
        'delivery cycle: ${cycle?.state.name ?? 'not started'}',
        'unserved: ${_listOrNone(unserved)}',
        'drainer: $drainerLine',
        'heartbeat: $heartbeatLine',
        for (final e in status.destinations.entries) _destinationLine(e),
      ].join('\n');
      if (!mounted) return;
      setState(() => _summary = text);
    } catch (_) {
      // Non-fatal.
    }
  }

  static String _destinationLine(
    MapEntry<String, DestinationDeliveryStatus> e,
  ) {
    final s = e.value;
    final halt = s.openHaltRequest?.purpose.wire ?? '-';
    final wedge = s.wedge?.cause.wire ?? '-';
    final guard = s.refillGuard == null ? '-' : 'set';
    return '  ${e.key}: halt $halt, wedge $wedge, refill guard $guard, '
        'unserved ${s.unserved?.wire ?? '-'}';
  }

  static String _listOrNone(List<String> items) =>
      items.isEmpty ? 'none' : items.join(', ');

  /// The first eight characters of a database identity, for display.
  static String _short(String id) => id.length <= 8 ? id : id.substring(0, 8);

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(color: DemoColors.bg, border: demoBorder),
      padding: const EdgeInsets.all(8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          const Text('DETAIL', style: DemoText.header),
          const SizedBox(height: 8),
          Expanded(child: SingleChildScrollView(child: _body())),
        ],
      ),
    );
  }

  Widget _body() {
    final aggId = widget.appState.selectedAggregateId;
    final eventId = widget.appState.selectedEventId;
    final fifoId = widget.appState.selectedFifoRowId;
    if (aggId != null) {
      return _AsyncJson(
        loader: () async {
          // Read the aggregate's row from the notes view.
          // AggregateFold stores: aggregateId, latestEventId, updatedAt,
          // firstEventTimestamp, sequence, plus event.data merged in.
          final rows = await widget.backend.findViewRows('notes');
          Map<String, Object?>? row;
          for (final r in rows) {
            if ((r['aggregateId'] as String?) == aggId) {
              row = r.cast<String, Object?>();
              break;
            }
          }
          if (row == null) return <String, Object?>{'error': 'not found'};
          return row;
        },
      );
    }
    if (eventId != null) {
      return _EventDetail(backend: widget.backend, eventId: eventId);
    }
    final fifoDestId = widget.appState.selectedFifoDestinationId;
    if (fifoId != null && fifoDestId != null) {
      return _AsyncJson(
        loader: () async {
          // FifoEntry.entryId == eventIds.first (library convention), so
          // rows collide on entry_id across destinations. Look up within
          // the specific destination the user selected.
          final entries = await widget.backend.listFifoEntries(fifoDestId);
          for (final entry in entries) {
            if (entry.entryId == fifoId) {
              return <String, Object?>{
                'destination': fifoDestId,
                ...entry.toJson(),
              };
            }
          }
          return <String, Object?>{
            'error': 'not found',
            'destination': fifoDestId,
            'entry_id': fifoId,
          };
        },
      );
    }
    // No selection — summary.
    final policy = widget.policyNotifier.value;
    return Text(
      '${_summary ?? 'loading...'}\n\n'
      'policy:\n'
      '  initialBackoff:    ${policy.initialBackoff.inMilliseconds}ms\n'
      '  backoffMultiplier: ${policy.backoffMultiplier}\n'
      '  maxBackoff:        ${policy.maxBackoff.inSeconds}s\n'
      '  jitterFraction:    ${policy.jitterFraction}\n'
      '  maxAttempts:       ${policy.maxAttempts}',
      style: DemoText.body,
    );
  }
}

/// Renders the selected event's metadata as JSON plus an explicit
/// per-provenance-entry summary that surfaces `origin_sequence_number`
/// when set. Most local events have a single origin-only provenance
/// entry with no `origin_sequence_number` — ingested events show one
/// per receiver hop with the originator's wire-supplied seq,
/// demonstrating the unified-store property.
///
/// `origin_sequence_number` is only rendered when non-null, keeping
/// local-event details uncluttered.
class _EventDetail extends StatefulWidget {
  const _EventDetail({required this.backend, required this.eventId});

  final SembastBackend backend;
  final String eventId;

  @override
  State<_EventDetail> createState() => _EventDetailState();
}

class _EventDetailState extends State<_EventDetail> {
  StoredEvent? _event;
  bool _loaded = false;

  @override
  void initState() {
    super.initState();
    _load();
  }

  @override
  void didUpdateWidget(_EventDetail old) {
    super.didUpdateWidget(old);
    if (old.eventId != widget.eventId) {
      _loaded = false;
      _event = null;
      _load();
    }
  }

  Future<void> _load() async {
    final events = await widget.backend.findAllEvents(limit: 100000);
    StoredEvent? event;
    for (final e in events) {
      if (e.eventId == widget.eventId) {
        event = e;
        break;
      }
    }
    if (!mounted) return;
    setState(() {
      _event = event;
      _loaded = true;
    });
  }

  @override
  Widget build(BuildContext context) {
    if (!_loaded) return const Text('...', style: DemoText.body);
    final event = _event;
    if (event == null) {
      return const Text('event not found', style: DemoText.body);
    }
    final provenance = _provenanceOf(event);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: <Widget>[
        if (provenance.isNotEmpty) ...<Widget>[
          const Text('provenance:', style: DemoText.body),
          for (var i = 0; i < provenance.length; i++)
            _ProvenanceLine(index: i, entry: provenance[i]),
          const SizedBox(height: 8),
        ],
        const Text('event:', style: DemoText.body),
        Text(_jsonOf(event), style: DemoText.body),
      ],
    );
  }

  static List<Map<String, Object?>> _provenanceOf(StoredEvent event) {
    final raw = event.metadata['provenance'];
    if (raw is! List) return const <Map<String, Object?>>[];
    return raw.cast<Map<String, Object?>>();
  }

  static String _jsonOf(StoredEvent event) {
    const encoder = JsonEncoder.withIndent('  ');
    return encoder.convert(event.toMap());
  }
}

class _ProvenanceLine extends StatelessWidget {
  const _ProvenanceLine({required this.index, required this.entry});

  final int index;
  final Map<String, Object?> entry;

  @override
  Widget build(BuildContext context) {
    final hop = entry['hop'] as String? ?? '?';
    final originSeq = entry['origin_sequence_number'] as int?;
    final ingestSeq = entry['ingest_sequence_number'] as int?;
    final summary = StringBuffer('  [$index] hop=$hop');
    if (ingestSeq != null) summary.write(' ingest_seq=$ingestSeq');
    if (originSeq != null) summary.write(' origin_seq=$originSeq');
    return Text(summary.toString(), style: DemoText.body);
  }
}

class _AsyncJson extends StatefulWidget {
  const _AsyncJson({required this.loader});

  final Future<Map<String, Object?>> Function() loader;

  @override
  State<_AsyncJson> createState() => _AsyncJsonState();
}

class _AsyncJsonState extends State<_AsyncJson> {
  Map<String, Object?>? _value;

  @override
  void initState() {
    super.initState();
    _load();
  }

  @override
  void didUpdateWidget(_AsyncJson old) {
    super.didUpdateWidget(old);
    _load();
  }

  Future<void> _load() async {
    final v = await widget.loader();
    if (!mounted) return;
    setState(() => _value = v);
  }

  @override
  Widget build(BuildContext context) {
    final v = _value;
    if (v == null) return const Text('...', style: DemoText.body);
    const encoder = JsonEncoder.withIndent('  ');
    return Text(encoder.convert(v), style: DemoText.body);
  }
}
