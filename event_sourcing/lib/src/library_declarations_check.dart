// Implements: EVS-DEV-causal-parents/T
// an append of an event type its entry type does not declare is refused; the
//   library appends to three application entry types, so each stage that does
//   so checks at construction that the registered definition declares every
//   event type the library appends, rather than failing when a rare stage
//   first fires.

import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:meta/meta.dart';

/// Throws [ArgumentError] unless [entryTypes] registers [entryTypeId] with a
/// definition declaring every event type in [required], naming the entry type
/// and the event types it lacks.
@internal
void requireLibraryDeclarations(
  EntryTypeRegistry entryTypes,
  String entryTypeId,
  List<EventTypeDeclaration> required,
) {
  final definition = entryTypes.byId(entryTypeId);
  final missing = <String>[
    for (final declaration in required)
      if (definition?.declarationFor(declaration.eventType) == null)
        declaration.eventType,
  ];
  if (missing.isEmpty) return;
  throw ArgumentError.value(
    entryTypeId,
    'entryTypeId',
    definition == null
        ? 'entry type is not registered; it must declare ${missing.join(', ')}'
        : 'entry type does not declare event types ${missing.join(', ')}; '
              'add their EventTypeDeclarations to its definition',
  );
}
