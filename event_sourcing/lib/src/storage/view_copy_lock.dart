// Implements: EVS-DEV-view-convergence/M (Sembast, isolate-local lock)
// outside the browser, a copy's lock is an entry in an isolate-local
//   registry keyed by (the backend's open database handle, the copy key),
//   taken synchronously and without waiting.
import 'package:meta/meta.dart' show internal;

/// Isolate-local registry of held catch-up copy locks: mirrors the drain
/// lock's per-handle registry, keyed instead by `(handle, copyKey)` so
/// distinct copies of one database catch up concurrently while each copy
/// admits one transaction at a time. On Sembast outside the browser this
/// is the whole of a copy's lock, since the library assumes one opener of
/// the database file; in the browser it is combined with the database's
/// Web Lock.
final Set<(Object, String)> _heldCopyLocks = {};

/// Tries the isolate-local lock of [copyKey] on [handle] without waiting;
/// false when another catch-up transaction in this isolate already holds
/// it.
@internal
bool tryLockViewCopyIsolate(Object handle, String copyKey) =>
    _heldCopyLocks.add((handle, copyKey));

/// Releases the isolate-local lock [tryLockViewCopyIsolate] granted.
/// Idempotent.
@internal
void unlockViewCopyIsolate(Object handle, String copyKey) {
  _heldCopyLocks.remove((handle, copyKey));
}
