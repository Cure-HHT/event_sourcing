// Verifies: EVS-PRD-portability/D
// classifyStorageException maps concrete
//   backend / platform error types (dart:io, sembast, dart:async) into the
//   abstract StorageException hierarchy so callers never see Sembast-specific
//   or IO-specific types.
import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/src/storage/storage_exception.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart';

void main() {
  group('classifyStorageException', () {
    // input; never throws.
    test('returns a StorageException for any input', () {
      final stack = StackTrace.current;
      // A pathological input: a plain object that is not an Exception.
      final weird = Object();
      final result = classifyStorageException(weird, stack);
      expect(result, isA<StorageException>());
    });

    // StorageTransientException.
    test('TimeoutException classifies as StorageTransientException', () {
      final stack = StackTrace.current;
      final error = TimeoutException(
        'op timed out',
        const Duration(seconds: 5),
      );
      final result = classifyStorageException(error, stack);
      expect(result, isA<StorageTransientException>());
      expect(identical(result.cause, error), isTrue);
      expect(identical(result.stackTrace, stack), isTrue);
    });

    // classifies as StorageCorruptException.
    test('FormatException on decode classifies as StorageCorruptException', () {
      final stack = StackTrace.current;
      const error = FormatException('bad JSON');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StorageCorruptException>());
    });

    // "hash chain" classifies as StorageCorruptException (same bucket, but
    // the wording exemplar from the REQ).
    test('hash-chain-mismatch FormatException classifies as corrupt', () {
      final stack = StackTrace.current;
      const error = FormatException(
        'hash chain break at sequence 42: previous_event_hash mismatch',
      );
      final result = classifyStorageException(error, stack);
      expect(result, isA<StorageCorruptException>());
      expect(result.message, contains('hash chain'));
    });

    // classifies as StoragePermanentException.
    test('FileSystemException classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      const error = FileSystemException(
        'permission denied',
        '/protected/db.db',
      );
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
    });

    test('StateError classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      final error = StateError('database is closed');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
    });

    test('ArgumentError classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      final error = ArgumentError('bad store name');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
    });

    // as StoragePermanentException (NEVER transient).
    test('unrecognized Object classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      final result = classifyStorageException(Object(), stack);
      expect(result, isA<StoragePermanentException>());
      // Crucially NOT transient — a retry loop on unknown errors is worse
      // than failing loudly.
      expect(result, isNot(isA<StorageTransientException>()));
    });

    // classifies as StoragePermanentException.
    test('bare Exception(...) classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      final error = Exception('unexpected backend error');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
    });

    // classifies as StoragePermanentException (lifecycle error, not
    // data-integrity).
    test('sembast DatabaseException.closed classifies as permanent', () {
      final stack = StackTrace.current;
      final error = DatabaseException.closed();
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
    });

    // classifies as StorageCorruptException. At this layer we cannot tell
    // "wrong codec configured" from "bytes damaged on disk"; the two are
    // caller-visible indistinguishable, so classify as corrupt.
    test('sembast DatabaseException.invalidCodec classifies as corrupt', () {
      final stack = StackTrace.current;
      final error = DatabaseException.invalidCodec('codec mismatch');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StorageCorruptException>());
    });

    // Exception subtypes) fall to the wildcard arm and classify as
    // StoragePermanentException. Covers the gap between the explicitly-
    // matched StateError / ArgumentError and the generic Object() case.
    test('AssertionError classifies as StoragePermanentException', () {
      final stack = StackTrace.current;
      final error = AssertionError('invariant violated');
      final result = classifyStorageException(error, stack);
      expect(result, isA<StoragePermanentException>());
      expect(result, isNot(isA<StorageTransientException>()));
      expect(identical(result.cause, error), isTrue);
    });

    // original cause and stackTrace by identity.
    test('classified result preserves cause and stackTrace', () {
      final stack = StackTrace.current;
      final inputs = <Object>[
        TimeoutException('t'),
        const FormatException('f'),
        const FileSystemException('fs', '/x'),
        StateError('s'),
        ArgumentError('a'),
        DatabaseException.closed(),
        Exception('bare'),
        Object(),
      ];
      for (final input in inputs) {
        final result = classifyStorageException(input, stack);
        expect(
          identical(result.cause, input),
          isTrue,
          reason: 'cause not preserved for ${input.runtimeType}',
        );
        expect(
          identical(result.stackTrace, stack),
          isTrue,
          reason: 'stackTrace not preserved for ${input.runtimeType}',
        );
      }
    });
  });
}
