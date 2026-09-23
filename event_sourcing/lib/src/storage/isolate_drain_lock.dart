// Implements: EVS-DEV-destination-drain-lock/A+C
// the drain lock of a backend over one open database handle: an
//   isolate-local registry keyed by the identity of the handle, checked and
//   set in one synchronous step; an acquisition that fails after it set its
//   entry removes the entry before the failure surfaces.
import 'dart:async';
import 'dart:collection';

import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// One handle's entry: the lock that holds it (null while an acquisition is
/// in progress) and a signal completed when the entry is removed.
final class _Entry {
  IsolateDrainLock? lock;
  final Completer<void> removed = Completer<void>();
}

/// The drain locks held in this isolate, by database handle. A top-level
/// variable is per isolate in Dart.
final LinkedHashMap<Object, _Entry> _entries =
    LinkedHashMap<Object, _Entry>.identity();

void _remove(Object handle, _Entry entry) {
  if (identical(_entries[handle], entry)) _entries.remove(handle);
  if (!entry.removed.isCompleted) entry.removed.complete();
}

/// Acquires the drain lock of [handle] (a database handle, compared by
/// identity) for [backend]. The check and the reservation of the entry are
/// one synchronous step. [bumpEpoch] raises the database's drain epoch in a
/// transaction and returns the value it stored; it receives a callback that
/// the backend calls inside that transaction after its write, for the
/// failure injection after the exclusion primitive is held.
@internal
Future<DrainLock> acquireIsolateDrainLock({
  required StorageBackend backend,
  required Object handle,
  required Future<int> Function(void Function() insideBump) bumpEpoch,
}) async {
  if (_entries.containsKey(handle)) {
    throw const DrainLockUnavailableException(
      'the drain lock of this database handle is held in this isolate',
    );
  }
  final hooks = DeliveryTestHooks.current;
  if (hooks?.failLockAcquisition?.call() ?? false) {
    throw const InjectedFailure('the drain lock acquisition');
  }
  final entry = _Entry();
  _entries[handle] = entry;
  try {
    final epoch = await bumpEpoch(() {
      if (DeliveryTestHooks.current?.failAfterExclusionObtained?.call() ??
          false) {
        throw const InjectedFailure(
          'the drain lock acquisition after the exclusion was obtained',
        );
      }
    });
    final lock = IsolateDrainLock._(backend, handle, entry, epoch);
    entry.lock = lock;
    return lock;
  } catch (_) {
    // Give up the exclusion before the failure surfaces.
    _remove(handle, entry);
    rethrow;
  }
}

/// Completes when the drain lock of [handle] held in this isolate is
/// released (at once when none is held).
@internal
Future<void> isolateDrainLockReleased(Object handle) {
  final entry = _entries[handle];
  return entry == null ? Future<void>.value() : entry.removed.future;
}

/// True while a drain lock of [handle] is held, or being acquired, in this
/// isolate.
@internal
bool isolateDrainLockHeld(Object handle) => _entries.containsKey(handle);

/// A drain lock held through [acquireIsolateDrainLock].
@internal
final class IsolateDrainLock implements DrainLock {
  IsolateDrainLock._(this._backend, this._handle, this._entry, this.epoch);

  final StorageBackend _backend;
  final Object _handle;
  final _Entry _entry;
  bool _released = false;
  bool _lossDetected = false;
  final Completer<void> _lost = Completer<void>();

  @override
  final int epoch;

  @override
  bool get isReleased => _released;

  @override
  Future<void> assertHeldInTxn(Transaction txn) async {
    if (_released) {
      throw const DrainLockLostException(
        DrainLockLossReason.released,
        'the drain lock was released',
      );
    }
    if (_lossDetected) {
      throw const DrainLockLostException(
        DrainLockLossReason.lossDetected,
        'a heartbeat of the drain lock failed',
      );
    }
    final stored = await _backend.readDrainEpochTxn(txn);
    if (stored != epoch) {
      throw DrainLockLostException(
        DrainLockLossReason.epochChanged,
        'the database stores drain epoch $stored; this holder acquired '
        '$epoch',
      );
    }
  }

  @override
  Future<void> assertHeld() => _backend.transaction(assertHeldInTxn);

  /// Nothing can take the lock from its holder while the isolate lives; a
  /// heartbeat fails only under the `failNextHeartbeat` test seam, and then
  /// reports the lock lost.
  @override
  Future<void> heartbeat() async {
    if (_released || _lossDetected) return;
    if (DeliveryTestHooks.current?.failNextHeartbeat?.call() ?? false) {
      _lossDetected = true;
      if (!_lost.isCompleted) _lost.complete();
      throw const InjectedFailure('the drain lock heartbeat');
    }
  }

  @override
  Future<void> release() async {
    if (_released) return;
    _released = true;
    _remove(_handle, _entry);
  }

  /// Completes only when a heartbeat fails (see [heartbeat]), never after
  /// [release].
  @override
  Future<void> get lost => _lost.future;
}
