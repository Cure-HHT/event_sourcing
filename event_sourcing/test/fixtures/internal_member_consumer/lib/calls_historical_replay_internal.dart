import 'package:event_sourcing/src/sync/historical_replay.dart';

/// Reaches the replay builders and the queue writer the fill commits them
/// through, through a `src/` import.
List<Object> replayEntryPoints() => <Object>[
  buildHistoricalReplayRows,
  writeQueueItemsTxn,
];
