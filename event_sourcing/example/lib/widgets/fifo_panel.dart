import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:event_sourcing/event_sourcing.dart'
    show
        Destination,
        DestinationSchedule,
        FifoEntry,
        HaltPurpose,
        HaltRequest,
        SetEndDateResult,
        StoredEvent,
        UserInitiator;
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/storage_watch.dart';
import 'package:event_sourcing_demo/widgets/styles.dart';
import 'package:event_sourcing_demo/widgets/wedges_panel.dart' show refusalText;
import 'package:flutter/material.dart';

class FifoPanel extends StatefulWidget {
  const FifoPanel({
    required this.destination,
    required this.watch,
    required this.appState,
    super.key,
  });

  /// The destination this panel renders. Either a `DemoDestination`
  /// (lossy 3rd-party shape: `transform()` produces opaque bytes the
  /// FIFO row stores under `wire_payload`) or any other [Destination]
  /// such as a native `esd/batch@2` destination (FIFO row stores
  /// `envelope_metadata` instead). The demo-specific
  /// connection / latency / batch-size knobs render whenever the
  /// destination implements [DemoKnobs] — both demo destinations do.
  final Destination destination;
  final StorageWatch watch;
  final AppState appState;

  @override
  State<FifoPanel> createState() => _FifoPanelState();
}

class _FifoPanelState extends State<FifoPanel> {
  List<FifoEntry> _rows = const <FifoEntry>[];
  Map<String, int> _seqByEventId = const <String, int>{};
  DestinationSchedule? _schedule;
  bool _opsOpen = false;
  String? _banner;
  HaltRequest? _openHalt;
  StreamSubscription<List<FifoEntry>>? _fifoSub;
  StreamSubscription<StoredEvent>? _eventsSub;
  Timer? _bannerTimer;

  final TextEditingController _startCtrl = TextEditingController();
  final TextEditingController _endCtrl = TextEditingController();

  /// Non-null when the destination implements [DemoKnobs], in which case
  /// the panel renders the live-tunable connection / latency / batch-size
  /// / accumulate knobs. Both `DemoDestination` (lossy) and
  /// `NativeDemoDestination` (esd/batch@2) implement DemoKnobs in the
  /// example app, so all three columns expose the same controls.
  /// Null for production destinations that don't carry these knobs.
  DemoKnobs? get _demo {
    final dest = widget.destination;
    return dest is DemoKnobs ? dest as DemoKnobs : null;
  }

  @override
  void initState() {
    super.initState();
    final demo = _demo;
    if (demo != null) {
      demo.connection.addListener(_onNotifier);
      demo.sendLatency.addListener(_onNotifier);
      demo.batchSize.addListener(_onNotifier);
      demo.maxAccumulateTimeN.addListener(_onNotifier);
    }
    widget.appState.addListener(_onNotifier);
    _fifoSub = widget.watch.queue(widget.destination.id).listen((rows) {
      if (!mounted) return;
      _onFifoSnapshot(rows);
    });
    // A halt request and its cancellation are events, not queue changes.
    _eventsSub = widget.watch.events().listen((_) {
      if (!mounted) return;
      unawaited(_reloadHalt());
    });
    unawaited(_reloadHalt());
  }

  /// Reads whether a halt request is open for this destination, from the
  /// persisted delivery status.
  Future<void> _reloadHalt() async {
    try {
      final status = await widget.appState.readDeliveryStatus();
      final open = status.destinations[widget.destination.id]?.openHaltRequest;
      if (!mounted) return;
      setState(() => _openHalt = open);
    } catch (_) {
      // Non-fatal.
    }
  }

  @override
  void dispose() {
    _fifoSub?.cancel();
    _eventsSub?.cancel();
    _bannerTimer?.cancel();
    final demo = _demo;
    if (demo != null) {
      demo.connection.removeListener(_onNotifier);
      demo.sendLatency.removeListener(_onNotifier);
      demo.batchSize.removeListener(_onNotifier);
      demo.maxAccumulateTimeN.removeListener(_onNotifier);
    }
    widget.appState.removeListener(_onNotifier);
    _startCtrl.dispose();
    _endCtrl.dispose();
    super.dispose();
  }

  void _onNotifier() {
    if (!mounted) return;
    setState(() {});
  }

  Future<void> _onFifoSnapshot(List<FifoEntry> rows) async {
    try {
      final schedule = await widget.appState.registry.scheduleOf(
        widget.destination.id,
      );
      // Per-row indexed lookup of the tail event's sequence_number for the
      // tile label. O(1) per row via the storage index, computed once per
      // FIFO snapshot so the synchronous tile builder stays sync.
      final seqByEventId = <String, int>{};
      for (final row in rows) {
        if (row.eventIds.isEmpty) continue;
        final tailId = row.eventIds.last;
        final tail = await widget.watch.reader.findEventById(tailId);
        if (tail != null) {
          seqByEventId[tailId] = tail.sequenceNumber;
        }
      }
      if (!mounted) return;
      setState(() {
        _rows = rows;
        _schedule = schedule;
        _seqByEventId = seqByEventId;
      });
    } catch (_) {
      // Non-fatal.
    }
  }

  Future<void> _reloadSchedule() async {
    try {
      final schedule = await widget.appState.registry.scheduleOf(
        widget.destination.id,
      );
      if (!mounted) return;
      setState(() => _schedule = schedule);
    } catch (_) {
      // Non-fatal.
    }
  }

  void _flashBanner(String msg) {
    if (!mounted) return;
    setState(() => _banner = msg);
    _bannerTimer?.cancel();
    _bannerTimer = Timer(const Duration(seconds: 2), () {
      if (!mounted) return;
      setState(() => _banner = null);
    });
  }

  String _scheduleLabel() {
    final s = _schedule;
    if (s == null) return 'LOADING';
    final now = DateTime.now().toUtc();
    if (s.startDate == null) return 'DORMANT';
    if (s.startDate!.isAfter(now)) {
      return 'SCHEDULED until ${s.startDate!.toIso8601String()}';
    }
    if (s.endDate == null) return 'ACTIVE';
    if (s.endDate!.isAfter(now)) {
      return 'ACTIVE until ${s.endDate!.toIso8601String()}';
    }
    return 'CLOSED @ ${s.endDate!.toIso8601String()}';
  }

  @override
  Widget build(BuildContext context) {
    final s = _schedule;
    final showStartEditor =
        s == null ||
        s.startDate == null ||
        s.startDate!.isAfter(DateTime.now().toUtc());
    final demo = _demo;
    final formatBadge = widget.destination.serializesNatively
        ? '[NATIVE ${widget.destination.wireFormat}]'
        : '[LOSSY ${widget.destination.wireFormat}]';
    return Container(
      decoration: BoxDecoration(color: DemoColors.bg, border: demoBorder),
      padding: const EdgeInsets.all(8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          // Controls section: scrollable so the card never overflows when
          // the pane height is small (e.g. both panes share the screen).
          SingleChildScrollView(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: <Widget>[
                Text(
                  widget.destination.id.toUpperCase(),
                  style: DemoText.header,
                ),
                Text(
                  formatBadge,
                  style: TextStyle(
                    color: widget.destination.serializesNatively
                        ? DemoColors.green
                        : DemoColors.accent,
                    fontFamily: 'monospace',
                    fontSize: 12,
                  ),
                ),
                Text(
                  _scheduleLabel(),
                  style: const TextStyle(
                    color: DemoColors.accent,
                    fontFamily: 'monospace',
                    fontSize: 14,
                  ),
                ),
                if (_openHalt != null)
                  Text(
                    'HALT REQUESTED (${_openHalt!.purpose.wire})',
                    style: const TextStyle(
                      color: DemoColors.wedged,
                      fontFamily: 'monospace',
                      fontSize: 14,
                    ),
                  ),
                if (showStartEditor) _startDateEditor(),
                _endDateEditor(),
                if (demo != null) _connectionDropdown(demo),
                if (demo != null) _latencySlider(demo),
                if (demo != null)
                  _sliderRow(
                    label: 'batch size',
                    value: demo.batchSize.value.toDouble(),
                    min: 1,
                    max: 12,
                    onChanged: (v) => demo.batchSize.value = v.round(),
                  ),
                if (demo != null)
                  _sliderRow(
                    label: 'accumulate (s)',
                    value: demo.maxAccumulateTimeN.value.inSeconds.toDouble(),
                    min: 0,
                    max: 20,
                    onChanged: (v) => demo.maxAccumulateTimeN.value = Duration(
                      seconds: v.round(),
                    ),
                  ),
                _opsDrawer(),
                if (_banner != null)
                  Container(
                    padding: const EdgeInsets.all(4),
                    color: DemoColors.accent,
                    child: Text(
                      _banner!,
                      style: const TextStyle(
                        color: DemoColors.bg,
                        fontSize: 12,
                      ),
                    ),
                  ),
              ],
            ),
          ),
          Expanded(child: _rowList()),
        ],
      ),
    );
  }

  Widget _startDateEditor() {
    return Row(
      children: <Widget>[
        Expanded(
          child: TextField(
            controller: _startCtrl,
            style: const TextStyle(color: DemoColors.fg, fontSize: 12),
            decoration: const InputDecoration(
              hintText: 'startDate ISO-8601 (e.g. 2026-04-23T00:00:00Z)',
              hintStyle: TextStyle(color: DemoColors.pending, fontSize: 11),
              isDense: true,
            ),
          ),
        ),
        TextButton(
          onPressed: () async {
            final d = DateTime.tryParse(_startCtrl.text);
            if (d == null) {
              _flashBanner('invalid date');
              return;
            }
            try {
              await widget.appState.registry.setStartDate(
                widget.destination.id,
                d.toUtc(),
                initiator: const UserInitiator('demo-user-1'),
              );
              _flashBanner('startDate set');
              await _reloadSchedule();
            } catch (e) {
              _flashBanner('err: $e');
            }
          },
          child: const Text('Set', style: TextStyle(color: DemoColors.accent)),
        ),
      ],
    );
  }

  Widget _endDateEditor() {
    return Row(
      children: <Widget>[
        Expanded(
          child: TextField(
            controller: _endCtrl,
            style: const TextStyle(color: DemoColors.fg, fontSize: 12),
            decoration: const InputDecoration(
              hintText: 'endDate, e.g. 2099-01-01T00:00:00Z (past = close)',
              hintStyle: TextStyle(color: DemoColors.pending, fontSize: 11),
              isDense: true,
            ),
          ),
        ),
        TextButton(
          onPressed: () async {
            final d = DateTime.tryParse(_endCtrl.text);
            if (d == null) {
              _flashBanner('invalid date');
              return;
            }
            try {
              final utc = d.toUtc();
              final result = await widget.appState.registry.setEndDate(
                widget.destination.id,
                utc,
                initiator: const UserInitiator('demo-user-1'),
              );
              final iso = utc.toIso8601String();
              final msg = switch (result) {
                SetEndDateResult.closed => 'closed at $iso',
                SetEndDateResult.scheduled => 'scheduled to close at $iso',
                SetEndDateResult.applied =>
                  'endDate set to $iso (no state change)',
              };
              _flashBanner(msg);
              await _reloadSchedule();
            } catch (e) {
              _flashBanner('err: $e');
            }
          },
          child: const Text('Set', style: TextStyle(color: DemoColors.accent)),
        ),
      ],
    );
  }

  Widget _connectionDropdown(DemoKnobs demo) {
    return Row(
      children: <Widget>[
        const Text('conn:', style: TextStyle(color: DemoColors.fg)),
        const SizedBox(width: 4),
        DropdownButton<Connection>(
          value: demo.connection.value,
          dropdownColor: DemoColors.bg,
          style: const TextStyle(color: DemoColors.fg),
          items: Connection.values
              .map(
                (c) =>
                    DropdownMenuItem<Connection>(value: c, child: Text(c.name)),
              )
              .toList(),
          onChanged: (c) {
            if (c == null) return;
            demo.connection.value = c;
          },
        ),
      ],
    );
  }

  Widget _sliderRow({
    required String label,
    required double value,
    required double min,
    required double max,
    required ValueChanged<double> onChanged,
    String? displayOverride,
  }) {
    return Row(
      children: <Widget>[
        SizedBox(
          width: 110,
          child: Text(
            label,
            style: const TextStyle(color: DemoColors.fg, fontSize: 12),
          ),
        ),
        Expanded(
          child: Slider(
            value: value.clamp(min, max),
            min: min,
            max: max,
            activeColor: DemoColors.accent,
            onChanged: onChanged,
          ),
        ),
        SizedBox(
          width: 54,
          child: Text(
            displayOverride ?? value.round().toString(),
            style: const TextStyle(color: DemoColors.fg, fontSize: 12),
          ),
        ),
      ],
    );
  }

  /// Non-linear latency slider: slider position `p` in [0, 1] maps to
  /// milliseconds `p^2 * 10000`. Small positions give small ms so
  /// single-digit / low-hundreds values are easy to dial in; the top
  /// of the slider reaches 10 s.
  Widget _latencySlider(DemoKnobs demo) {
    const maxMs = 10000.0;
    final currentMs = demo.sendLatency.value.inMilliseconds.toDouble();
    final position = math.sqrt((currentMs / maxMs).clamp(0.0, 1.0));
    return _sliderRow(
      label: 'latency (ms)',
      value: position,
      min: 0,
      max: 1,
      displayOverride: currentMs.round().toString(),
      onChanged: (p) {
        final ms = (p * p * maxMs).round();
        demo.sendLatency.value = Duration(milliseconds: ms);
      },
    );
  }

  Widget _opsDrawer() {
    return Column(
      children: <Widget>[
        Align(
          alignment: Alignment.centerLeft,
          child: TextButton(
            onPressed: () => setState(() => _opsOpen = !_opsOpen),
            child: Text(
              _opsOpen ? 'ops ▾' : 'ops ▸',
              style: const TextStyle(color: DemoColors.accent),
            ),
          ),
        ),
        if (_opsOpen)
          Wrap(
            children: <Widget>[
              TextButton(
                onPressed: () => _halt(HaltPurpose.pause),
                child: const Text(
                  '[Halt]',
                  style: TextStyle(color: DemoColors.accent),
                ),
              ),
              TextButton(
                onPressed: () => _halt(HaltPurpose.reconfigure),
                child: const Text(
                  '[Halt to reconfigure]',
                  style: TextStyle(color: DemoColors.accent),
                ),
              ),
              TextButton(
                onPressed: _cancelHalt,
                child: const Text(
                  '[Cancel halt]',
                  style: TextStyle(color: DemoColors.accent),
                ),
              ),
              if (widget.destination is DemoDestination &&
                  narrowFilter(widget.destination.filter) != null)
                TextButton(
                  onPressed: _reconfigureDrainer,
                  child: const Text(
                    '[Reconfigure drainer]',
                    style: TextStyle(color: DemoColors.accent),
                  ),
                ),
              if (widget.destination.allowHardDelete)
                TextButton(
                  onPressed: () async {
                    try {
                      await widget.appState.deleteDestination(
                        widget.destination.id,
                      );
                      _flashBanner('deleted');
                    } catch (e) {
                      // A refusal (for example while the queue head is
                      // pending and may be in delivery) names its reason.
                      _flashBanner(
                        e is StateError
                            ? 'delete refused: ${e.message} Use [Halt]: '
                                  'the drainer wedges the head, and the '
                                  'deletion is accepted then.'
                            : 'delete err: $e',
                      );
                    }
                  },
                  child: const Text(
                    '[Delete destination]',
                    style: TextStyle(color: DemoColors.red),
                  ),
                ),
            ],
          ),
      ],
    );
  }

  Future<void> _halt(HaltPurpose purpose) async {
    try {
      await widget.appState.requestHalt(widget.destination.id, purpose);
      _flashBanner('halt requested (${purpose.wire})');
    } catch (e) {
      _flashBanner('halt refused: ${refusalText(e)}');
    }
    await _reloadHalt();
  }

  Future<void> _cancelHalt() async {
    try {
      await widget.appState.cancelHalt(widget.destination.id);
      _flashBanner('halt cancelled');
    } catch (e) {
      _flashBanner('cancel refused: ${refusalText(e)}');
    }
    await _reloadHalt();
  }

  /// Plays a deployment of a new configuration of this destination: the
  /// pane's drainer restarts with the destination's filter narrowed.
  Future<void> _reconfigureDrainer() async {
    final narrowed = narrowFilter(widget.destination.filter);
    if (narrowed == null) return;
    try {
      await widget.appState.reconfigureDrainer(widget.destination.id, narrowed);
      _flashBanner(
        'drainer reconfigured: ${narrowed.entryTypes!.join(', ')} only',
      );
    } catch (e) {
      _flashBanner('reconfigure failed: ${refusalText(e)}');
    }
  }

  Widget _rowList() {
    // Display reversed (descending sequence_in_queue) so the most
    // recently enqueued row is on top.
    final display = <FifoEntry>[..._rows]
      ..sort((a, b) => b.sequenceInQueue.compareTo(a.sequenceInQueue));
    return ListView.builder(
      itemCount: display.length,
      itemBuilder: (context, i) => _FifoRowTile(
        row: display[i],
        seqByEventId: _seqByEventId,
        selected:
            widget.appState.selectedFifoRowId == display[i].entryId &&
            widget.appState.selectedFifoDestinationId == widget.destination.id,
        onTap: () => widget.appState.selectFifoRow(
          widget.destination.id,
          display[i].entryId,
        ),
        destinationId: widget.destination.id,
        onTombstoneAndRefill: () async {
          try {
            await widget.appState.recover(
              widget.destination.id,
              display[i].entryId,
            );
            _flashBanner('tombstoned & refilled');
            // FIFO mutation: the queue watch emits a fresh snapshot which
            // _onFifoSnapshot consumes — no explicit refresh needed.
          } catch (e) {
            _flashBanner('recovery refused: ${refusalText(e)}');
          }
        },
      ),
    );
  }
}

class _FifoRowTile extends StatelessWidget {
  const _FifoRowTile({
    required this.row,
    required this.seqByEventId,
    required this.selected,
    required this.onTap,
    required this.destinationId,
    required this.onTombstoneAndRefill,
  });

  final FifoEntry row;
  final Map<String, int> seqByEventId;
  final bool selected;
  final VoidCallback onTap;
  final String destinationId;
  final VoidCallback onTombstoneAndRefill;

  @override
  Widget build(BuildContext context) {
    final seq = row.sequenceInQueue;
    final status = row.finalStatus?.toJson();
    final attemptsLen = row.attempts.length;
    final eventIds = row.eventIds;
    final count = eventIds.length;
    final latestSeq = eventIds.isNotEmpty ? seqByEventId[eventIds.last] : null;
    final latestLabel = latestSeq != null ? '#$latestSeq' : '-';

    String prefix;
    Color color;
    if (status == 'sent') {
      prefix = '[SENT] ';
      color = DemoColors.sent;
    } else if (status == 'wedged') {
      prefix = '[wdg]  ';
      color = DemoColors.wedged;
    } else if (status == 'tombstoned') {
      prefix = '[tmb]  ';
      color = DemoColors.wedged;
    } else if (attemptsLen > 0) {
      prefix = '> ';
      color = DemoColors.retrying;
    } else {
      prefix = '[pend] ';
      color = DemoColors.fg;
    }

    final label =
        '$prefix#$seq: events: $count (latest: $latestLabel)  attempts:$attemptsLen';
    // Per-row format badge + storage-shape summary. Native rows
    // carry envelope_metadata + null wire_payload — the on-wire bytes are
    // reconstructed deterministically by drain at send time, so the storage
    // footprint is just the envelope identity. Lossy 3rd-party rows persist
    // the decoded JSON map produced by `transform()`; re-encoding it gives
    // a stable byte count that approximates the on-wire size for the demo.
    final envelope = row.envelopeMetadata;
    final isNative = envelope != null;
    final badge = isNative ? '[NATIVE]' : '[LOSSY]';
    final badgeColor = isNative ? DemoColors.green : DemoColors.accent;
    final String shapeSummary;
    if (isNative) {
      final batchPrefix = envelope.batchId.length >= 6
          ? envelope.batchId.substring(0, 6)
          : envelope.batchId;
      shapeSummary =
          'batch $batchPrefix | $count events | wire bytes recovered on demand';
    } else {
      // wire_payload is the decoded JSON map the delivery cycle stored;
      // re-encode it via JSON to get a representative byte count
      // comparable to the on-wire size. This is the same shape
      // `transform()` produced, so the count stays comparable across
      // both storage shapes for the demo's pedagogy.
      final payload = row.wirePayload;
      final bytesLabel = payload == null
          ? '?'
          : utf8.encode(jsonEncode(payload)).length.toString();
      shapeSummary = 'wire_payload: $bytesLabel bytes';
    }
    // Show the TombstoneAndRefill button only on wedged rows: recovery
    // requires a wedged head.
    final isWedged = status == 'wedged';
    return InkWell(
      onTap: onTap,
      child: Container(
        decoration: BoxDecoration(
          color: selected ? DemoColors.selected : DemoColors.bg,
          border: selected ? demoSelectedBorder : null,
        ),
        padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
        child: Row(
          children: <Widget>[
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: <Widget>[
                  Text(
                    label,
                    style: TextStyle(
                      color: color,
                      fontFamily: 'monospace',
                      fontSize: 14,
                    ),
                  ),
                  Row(
                    children: <Widget>[
                      Text(
                        badge,
                        style: TextStyle(
                          color: badgeColor,
                          fontFamily: 'monospace',
                          fontSize: 11,
                        ),
                      ),
                      const SizedBox(width: 6),
                      Expanded(
                        child: Text(
                          shapeSummary,
                          style: const TextStyle(
                            color: DemoColors.pending,
                            fontFamily: 'monospace',
                            fontSize: 11,
                          ),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
            if (isWedged)
              ElevatedButton(
                onPressed: onTombstoneAndRefill,
                child: const Text(
                  'Tombstone & Refill',
                  style: TextStyle(fontSize: 12),
                ),
              ),
          ],
        ),
      ),
    );
  }
}
