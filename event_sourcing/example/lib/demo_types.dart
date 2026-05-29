import 'package:event_sourcing/event_sourcing.dart';

/// Regular diary-like entry type. Materializes into the `notes` view via
/// the `AggregateProjectionSpec` registered in `_bootstrapPane`.
const EntryTypeDefinition demoNoteType = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: 1,
  name: 'Demo note',
);

/// Red-button action event. Point-in-time; no materialized view row.
const EntryTypeDefinition redButtonType = EntryTypeDefinition(
  id: 'red_button_pressed',
  registeredVersion: 1,
  name: 'Red button pressed',
);

/// Green-button action event.
const EntryTypeDefinition greenButtonType = EntryTypeDefinition(
  id: 'green_button_pressed',
  registeredVersion: 1,
  name: 'Green button pressed',
);

/// Blue-button action event.
const EntryTypeDefinition blueButtonType = EntryTypeDefinition(
  id: 'blue_button_pressed',
  registeredVersion: 1,
  name: 'Blue button pressed',
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
