import 'package:event_sourcing/event_sourcing.dart';

// Implements: REQ-d00116 — EntryTypeDefinition value type from
// event_sourcing_datastore; demo instances registered via
// bootstrapAppendOnlyDatastore (REQ-d00134) at Task 9.
// Validated by: JNY-01 (demo_note lifecycle), JNY-02 (CQRS via action
// types' distinct aggregate types in demoAggregateTypeByEntryTypeId).

/// Regular diary-like entry type. Materializes into `diary_entries` via
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
/// `bootstrapAppendOnlyDatastore` at Task 9 (REQ-d00134).
const List<EntryTypeDefinition> allDemoEntryTypes = <EntryTypeDefinition>[
  demoNoteType,
  redButtonType,
  greenButtonType,
  blueButtonType,
];

/// Per-entry-type aggregate-type lookup.
///
/// `EntryTypeDefinition` itself does not carry an `aggregateType` field
/// (REQ-d00116 shape). `EventStore.append` (REQ-d00141-B) takes
/// `aggregateType` as a per-call argument; the demo looks it up here
/// keyed on `entryType.id`. Distinct aggregate types on the three action
/// events are the CQRS discriminator JNY-02 walks through the EVENTS
/// panel.
const Map<String, String> demoAggregateTypeByEntryTypeId = <String, String>{
  'demo_note': 'DiaryEntry',
  'red_button_pressed': 'RedButtonPressed',
  'green_button_pressed': 'GreenButtonPressed',
  'blue_button_pressed': 'BlueButtonPressed',
};
