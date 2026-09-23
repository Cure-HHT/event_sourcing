import 'package:event_sourcing/src/sync/historical_replay.dart';

/// Reaches the replay that enqueues and rewinds a fill position through a
/// `src/` import.
List<Object> replayEntryPoints() => <Object>[runHistoricalReplay, runGapReplay];
