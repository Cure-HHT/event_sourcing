import 'package:event_sourcing/src/event_store.dart';

/// Reaches the replay builders and the queue writer the fill commits them
/// through, through a `src/` import.
List<Object> replayEntryPoints() => <Object>[
  buildHistoricalReplayRows,
  writeQueueItemsTxn,
];
