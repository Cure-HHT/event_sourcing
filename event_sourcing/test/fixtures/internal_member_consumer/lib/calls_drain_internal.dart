import 'package:event_sourcing/src/event_store.dart';

/// Reaches the drain through a `src/` import.
Object drainEntryPoint() => drain;

/// Reaches the drain's halt honour through a `src/` import.
Object haltEntryPoint() => honourHaltById;
