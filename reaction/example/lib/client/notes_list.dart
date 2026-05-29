// reaction/example/lib/client/notes_list.dart
//
// Reactive notes_today list, rendered on top of reaction_widgets'
// headless `ViewBuilder<Note>`.
//
// ViewBuilder owns everything the old hand-rolled `_NotesAccumulator`
// used to do — subscribing through the composed scope's ViewSource,
// accumulating the `Snapshot × N -> EndOfReplay -> Delta / Tombstone`
// stream keyed by aggregate id, and surfacing a sealed `ViewState<Note>`
// (Loading / Ready / Stale). This file is now pure rendering sugar: it
// maps each ViewState variant to a Flutter widget. That is exactly the
// "headless primitive + per-app sugar" split the widget library is built
// around (EVS-PRD-reaction-widget-contract-G).
//
// The `Stale` branch demonstrates the transport-aware affordance: when
// the WS drops, ViewBuilder keeps the last-known rows (driven by the
// scope's authoritative ConnectionStatus, NOT by inferring stream
// liveness — EVS-PRD-reaction-widget-contract-I) so we render a
// "reconnecting" banner over stale data instead of blanking the list.

// `material.dart` also exports a `ViewBuilder` (from SearchAnchor); hide it
// so the unqualified name resolves to reaction_widgets' view primitive.
import 'package:flutter/material.dart' hide ViewBuilder;
import 'package:reaction_widgets/reaction_widgets.dart';

import 'ui.dart';

/// One materialised `notes_today` row.
class Note {
  const Note({required this.id, required this.workspace, required this.title});

  final String id;
  final String workspace;
  final String title;

  /// Maps a raw view-row map to a [Note]. The substrate's AggregateFold
  /// stamps the aggregate id under the `'aggregateId'` key before the
  /// mapper runs (a Layer-2 convention), so we read it from there.
  static Note fromRow(Map<String, Object?> row) => Note(
    id: row['aggregateId']! as String,
    workspace: (row['workspace'] as String?) ?? '(unknown)',
    title: (row['title'] as String?) ?? '(untitled)',
  );
}

class NotesList extends StatelessWidget {
  const NotesList({super.key});

  @override
  Widget build(BuildContext context) {
    return ViewBuilder<Note>(
      viewName: 'notes_today',
      mapper: Note.fromRow,
      aggregateIdOf: (note) => note.id,
      builder: (context, state) => switch (state) {
        Loading<Note>() => const Center(child: CircularProgressIndicator()),
        Ready<Note>(:final rows) => _NoteListView(notes: rows),
        Stale<Note>(:final lastRows) => Column(
          children: <Widget>[
            _StaleBanner(),
            Expanded(child: _NoteListView(notes: lastRows)),
          ],
        ),
      },
    );
  }
}

/// Reconnecting banner shown over the last-known rows (Stale state).
class _StaleBanner extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      margin: const EdgeInsets.only(bottom: 8),
      decoration: BoxDecoration(
        color: scheme.tertiaryContainer,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: <Widget>[
          SizedBox(
            width: 14,
            height: 14,
            child: CircularProgressIndicator(
              strokeWidth: 2,
              color: scheme.onTertiaryContainer,
            ),
          ),
          const SizedBox(width: 10),
          Text(
            'Reconnecting — showing last-known notes…',
            style: TextStyle(color: scheme.onTertiaryContainer),
          ),
        ],
      ),
    );
  }
}

/// Pure rendering of a row set. ViewBuilder does not sort, so we sort by
/// id here for a stable display order.
class _NoteListView extends StatelessWidget {
  const _NoteListView({required this.notes});

  final List<Note> notes;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    if (notes.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: <Widget>[
            Icon(Icons.notes_outlined, size: 40, color: scheme.outline),
            const SizedBox(height: 8),
            Text('(no notes yet)', style: TextStyle(color: scheme.outline)),
          ],
        ),
      );
    }
    final sorted = notes.toList()..sort((a, b) => a.id.compareTo(b.id));
    return ListView.separated(
      padding: const EdgeInsets.symmetric(vertical: 4),
      itemCount: sorted.length,
      separatorBuilder: (_, _) => const SizedBox(height: 8),
      itemBuilder: (context, i) {
        final note = sorted[i];
        final color = workspaceColor(note.workspace);
        return Card(
          elevation: 0,
          margin: EdgeInsets.zero,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(14),
            side: BorderSide(color: scheme.outlineVariant),
          ),
          child: ListTile(
            leading: CircleAvatar(
              backgroundColor: color.withValues(alpha: 0.15),
              foregroundColor: color,
              child: const Icon(Icons.sticky_note_2_outlined),
            ),
            title: Text(
              note.title,
              style: const TextStyle(fontWeight: FontWeight.w600),
            ),
            subtitle: Text(note.id, style: const TextStyle(fontSize: 11)),
            trailing: TagChip(label: note.workspace, color: color),
          ),
        );
      },
    );
  }
}
