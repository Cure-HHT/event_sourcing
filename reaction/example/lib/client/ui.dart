// reaction/example/lib/client/ui.dart
//
// App-level visual sugar. NONE of this lives in `reaction_widgets` — the
// headless layer ships state machines and builder callbacks but no
// rendered/styled widgets (EVS-PRD-reaction-widget-contract-G). Every
// Card, Chip, color, and icon here is the consumer app dressing the
// headless builders, so the look changes without touching
// reaction_widgets or anything below it.
//
// Declaring icons explicitly (via const `Icons.*` referenced from app
// code) also keeps Flutter web's icon tree-shaker from dropping the
// glyphs — the cause of the "tofu" boxes that a bare default
// DropdownButton arrow showed in the release web build.

import 'package:flutter/material.dart';

/// Stable, distinct color per demo workspace so a note's origin is
/// readable at a glance. Unknown workspaces fall back to a neutral tone.
Color workspaceColor(String workspace) => switch (workspace) {
  'west' => const Color(0xFF1565C0), // blue
  'east' => const Color(0xFF2E7D32), // green
  'mars' => const Color(0xFFC62828), // red
  _ => const Color(0xFF607D8B), // blue-grey
};

/// A titled, outlined panel used to frame each app section. Carries an
/// explicit leading [icon] (declared by the caller, so the glyph is
/// retained by tree-shaking).
class SectionCard extends StatelessWidget {
  const SectionCard({
    required this.title,
    required this.icon,
    required this.child,
    this.accent,
    super.key,
  });

  final String title;
  final IconData icon;
  final Widget child;
  final Color? accent;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final color = accent ?? scheme.primary;
    return Card(
      elevation: 0,
      margin: EdgeInsets.zero,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
        side: BorderSide(color: scheme.outlineVariant),
      ),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: <Widget>[
            Row(
              children: <Widget>[
                Icon(icon, size: 18, color: color),
                const SizedBox(width: 8),
                Text(
                  title,
                  style: Theme.of(context).textTheme.titleSmall?.copyWith(
                    color: color,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            child,
          ],
        ),
      ),
    );
  }
}

/// A colored pill used for workspace tags, permissions, etc.
class TagChip extends StatelessWidget {
  const TagChip({
    required this.label,
    required this.color,
    this.icon,
    super.key,
  });

  final String label;
  final Color color;
  final IconData? icon;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.12),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: color.withValues(alpha: 0.5)),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: <Widget>[
          if (icon != null) ...<Widget>[
            Icon(icon, size: 13, color: color),
            const SizedBox(width: 4),
          ],
          Text(
            label,
            style: TextStyle(
              color: color,
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}
