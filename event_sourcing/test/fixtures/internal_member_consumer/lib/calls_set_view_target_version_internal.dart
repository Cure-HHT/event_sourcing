import 'package:event_sourcing/event_sourcing.dart';

/// Writes a view target version outside boot seeding.
Future<void> setTarget(EventStoreBundle bundle) =>
    bundle.setViewTargetVersion('view', 'entry', const EntryTypeVersion(1, 0));
