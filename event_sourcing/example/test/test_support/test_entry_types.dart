// The demo's entry types as the tests register them: each button type also
// declares `pressed`, the event type the tests append for a press that is
// not the action bar's own `finalized`.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_types.dart';

/// [allDemoEntryTypes], each button type declaring `pressed` beside its own
/// event types.
final List<EntryTypeDefinition> testDemoEntryTypes = <EntryTypeDefinition>[
  for (final definition in allDemoEntryTypes)
    definition.id == demoNoteType.id
        ? definition
        : EntryTypeDefinition(
            id: definition.id,
            registeredVersion: definition.registeredVersion,
            name: definition.name,
            patch: definition.patch,
            declarations: <EventTypeDeclaration>[
              ...definition.declarations,
              const EventTypeDeclaration(
                eventType: 'pressed',
                kind: CausalKind.version,
                eligible: true,
              ),
            ],
          ),
];
