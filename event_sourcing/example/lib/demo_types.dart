import 'package:event_sourcing/event_sourcing.dart';

/// The event types of a demo note, each an eligible version: the action
/// bar records a checkpoint, a finalized note and a tombstone.
const List<EventTypeDeclaration> _noteDeclarations = <EventTypeDeclaration>[
  EventTypeDeclaration(
    eventType: 'checkpoint',
    kind: CausalKind.version,
    eligible: true,
  ),
  EventTypeDeclaration(
    eventType: 'finalized',
    kind: CausalKind.version,
    eligible: true,
  ),
  EventTypeDeclaration(
    eventType: 'tombstone',
    kind: CausalKind.version,
    eligible: true,
  ),
];

/// The event types of a button press, each an eligible version: the action
/// bar records a press as finalized, and a press may be recorded as
/// pressed.
const List<EventTypeDeclaration> _buttonDeclarations = <EventTypeDeclaration>[
  EventTypeDeclaration(
    eventType: 'finalized',
    kind: CausalKind.version,
    eligible: true,
  ),
  EventTypeDeclaration(
    eventType: 'pressed',
    kind: CausalKind.version,
    eligible: true,
  ),
];

/// Regular diary-like entry type. Materializes into the `notes` view via
/// the `AggregateProjectionSpec` registered in `_bootstrapPane`.
const EntryTypeDefinition demoNoteType = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Demo note',
  declarations: _noteDeclarations,
);

/// Red-button action event. Point-in-time; no materialized view row.
const EntryTypeDefinition redButtonType = EntryTypeDefinition(
  id: 'red_button_pressed',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Red button pressed',
  declarations: _buttonDeclarations,
);

/// Green-button action event.
const EntryTypeDefinition greenButtonType = EntryTypeDefinition(
  id: 'green_button_pressed',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Green button pressed',
  declarations: _buttonDeclarations,
);

/// Blue-button action event.
const EntryTypeDefinition blueButtonType = EntryTypeDefinition(
  id: 'blue_button_pressed',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Blue button pressed',
  declarations: _buttonDeclarations,
);

/// Full demo entry-type set. Passed as `entryTypes:` to
/// `bootstrapEventStore`.
const List<EntryTypeDefinition> allDemoEntryTypes = <EntryTypeDefinition>[
  demoNoteType,
  redButtonType,
  greenButtonType,
  blueButtonType,
];

/// Per-entry-type aggregate-type lookup.
///
/// `EntryTypeDefinition` itself does not carry an `aggregateType` field.
/// `EventStore.append` takes `aggregateType` as a per-call argument; the
/// demo looks it up here keyed on `entryType.id`. Distinct aggregate types
/// on the three action events serve as the CQRS discriminator visible in
/// the EVENTS panel.
const Map<String, String> demoAggregateTypeByEntryTypeId = <String, String>{
  'demo_note': 'Note',
  'red_button_pressed': 'RedButtonPressed',
  'green_button_pressed': 'GreenButtonPressed',
  'blue_button_pressed': 'BlueButtonPressed',
};
