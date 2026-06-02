// example_clinical_scopes/lib/client/participants_list.dart
//
// Reactive `participants` list, rendered on top of reaction_widgets'
// headless `ViewBuilder<Participant>`. Mirrors `reaction/example`'s
// notes_list.dart.
//
// The rows arriving here are already narrowed to the logged-in user's
// scope by the server's ScopeDescendantExpander — an Investigator at
// site-A/B receives only their participants, an Overseer receives the
// whole region via two containment hops, an Admin receives all. This
// file is pure rendering sugar: it maps each ViewState variant to a
// Flutter widget (the headless-primitive + per-app-sugar split).

// `material.dart` also exports a `ViewBuilder` (from SearchAnchor); hide it
// so the unqualified name resolves to reaction_widgets' view primitive.
import 'package:flutter/material.dart' hide ViewBuilder;
import 'package:reaction_widgets/reaction_widgets.dart';

import 'package:example_clinical_scopes/shared/clinical_types.dart';

class ParticipantsList extends StatelessWidget {
  const ParticipantsList({super.key});

  @override
  Widget build(BuildContext context) {
    return ViewBuilder<Participant>(
      viewName: 'participants',
      mapper: Participant.fromRow,
      aggregateIdOf: (p) => p.id,
      builder: (context, state) => switch (state) {
        Loading<Participant>() => const Center(
          child: CircularProgressIndicator(),
        ),
        Ready<Participant>(:final rows) => _ParticipantListView(
          participants: rows,
        ),
        Stale<Participant>(:final lastRows) => Column(
          children: <Widget>[
            const _StaleBanner(),
            Expanded(child: _ParticipantListView(participants: lastRows)),
          ],
        ),
      },
    );
  }
}

/// Reconnecting banner shown over the last-known rows (Stale state).
class _StaleBanner extends StatelessWidget {
  const _StaleBanner();

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
            'Reconnecting — showing last-known participants…',
            style: TextStyle(color: scheme.onTertiaryContainer),
          ),
        ],
      ),
    );
  }
}

/// Pure rendering of a row set. ViewBuilder does not sort, so we sort by
/// id here for a stable display order.
class _ParticipantListView extends StatelessWidget {
  const _ParticipantListView({required this.participants});

  final List<Participant> participants;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    if (participants.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: <Widget>[
            Icon(Icons.people_outline, size: 40, color: scheme.outline),
            const SizedBox(height: 8),
            Text(
              '(no participants in scope)',
              style: TextStyle(color: scheme.outline),
            ),
          ],
        ),
      );
    }
    final sorted = participants.toList()..sort((a, b) => a.id.compareTo(b.id));
    return ListView.separated(
      padding: const EdgeInsets.symmetric(vertical: 4),
      itemCount: sorted.length,
      separatorBuilder: (_, _) => const SizedBox(height: 8),
      itemBuilder: (context, i) {
        final p = sorted[i];
        return Card(
          elevation: 0,
          margin: EdgeInsets.zero,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(14),
            side: BorderSide(color: scheme.outlineVariant),
          ),
          child: ListTile(
            leading: CircleAvatar(
              backgroundColor: scheme.primaryContainer,
              foregroundColor: scheme.onPrimaryContainer,
              child: const Icon(Icons.person_outline),
            ),
            title: Text(
              p.name,
              style: const TextStyle(fontWeight: FontWeight.w600),
            ),
            subtitle: Text(p.id, style: const TextStyle(fontSize: 11)),
            trailing: Chip(label: Text(p.siteId)),
          ),
        );
      },
    );
  }
}
