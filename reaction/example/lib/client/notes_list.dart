// reaction/example/lib/client/notes_list.dart
//
// Reactive notes_today list. Subscribes to the substrate's notes_today
// view through the RemoteScope's ViewSource and accumulates the
// Snapshot/EndOfReplay/Delta/Tombstone stream into a flat list keyed by
// aggregate id. Rows render with the workspace as a tag chip.
//
// The accumulator buffers snapshots until EndOfReplay before
// publishing, so the list updates atomically on each re-subscription
// (no flash of "first snapshot only" while replay is still streaming).

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';

class Note {
  const Note({required this.id, required this.workspace, required this.title});

  final String id;
  final String workspace;
  final String title;
}

class NotesList extends StatefulWidget {
  const NotesList({required this.viewSource, super.key});

  final ViewSource viewSource;

  @override
  State<NotesList> createState() => _NotesListState();
}

class _NotesListState extends State<NotesList> {
  final _NotesAccumulator _notes = _NotesAccumulator();
  late final StreamSubscription<Update<Note>> _sub;

  @override
  void initState() {
    super.initState();
    final stream = widget.viewSource.watch<Note>(
      viewName: 'notes_today',
      mapper: (row) => Note(
        id: row['aggregateId']! as String,
        workspace: (row['workspace'] as String?) ?? '(unknown)',
        title: (row['title'] as String?) ?? '(untitled)',
      ),
    );
    _sub = stream.listen(
      _notes.apply,
      onError: (Object err) {
        // Errors surface in the parent screen's flash via the parent;
        // local UI just keeps the most recent state.
      },
    );
    _notes.addListener(_onChanged);
  }

  void _onChanged() {
    if (mounted) setState(() {});
  }

  @override
  void dispose() {
    _notes.removeListener(_onChanged);
    unawaited(_sub.cancel());
    _notes.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (!_notes.replayComplete) {
      return const Center(child: CircularProgressIndicator());
    }
    final notes = _notes.notes;
    if (notes.isEmpty) {
      return const Center(child: Text('(no notes yet)'));
    }
    return ListView.separated(
      itemCount: notes.length,
      separatorBuilder: (_, _) => const Divider(height: 0),
      itemBuilder: (context, i) {
        final note = notes[i];
        return ListTile(
          dense: true,
          leading: Chip(
            label: Text(note.workspace),
            visualDensity: VisualDensity.compact,
          ),
          title: Text(note.title),
          subtitle: Text(note.id, style: const TextStyle(fontSize: 11)),
        );
      },
    );
  }
}

/// Buffers Snapshot/Delta/Tombstone into a Map until EndOfReplay; then
/// atomic swap. After EOR, mutations apply directly. Prevents the
/// "partial replay flash" on resubscribe.
class _NotesAccumulator extends ChangeNotifier {
  Map<String, Note> _staging = <String, Note>{};
  Map<String, Note> _published = <String, Note>{};
  bool _replayComplete = false;

  bool get replayComplete => _replayComplete;
  List<Note> get notes =>
      (_published.values.toList()..sort((a, b) => a.id.compareTo(b.id)));

  void apply(Update<Note> update) {
    switch (update) {
      case Snapshot<Note>(:final value):
        if (value != null) _staging[value.id] = value;
      case EndOfReplay<Note>():
        _published = _staging;
        _staging = <String, Note>{};
        _replayComplete = true;
        notifyListeners();
      case Delta<Note>(:final value):
        if (_replayComplete) {
          _published = Map<String, Note>.from(_published)..[value.id] = value;
          notifyListeners();
        } else {
          _staging[value.id] = value;
        }
      case Tombstone<Note>(:final aggregateId):
        if (_replayComplete) {
          if (_published.containsKey(aggregateId)) {
            _published = Map<String, Note>.from(_published)
              ..remove(aggregateId);
            notifyListeners();
          }
        } else {
          _staging.remove(aggregateId);
        }
    }
  }
}
