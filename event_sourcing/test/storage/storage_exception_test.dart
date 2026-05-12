// Verifies: EVS-PRD-portability/D — StorageException sealed hierarchy hides
//   backend-specific error types; three-variant exhaustive pattern match
//   enforced at compile time.
import 'package:event_sourcing/src/storage/storage_exception.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('StorageException', () {
    // the three variants at compile time; the analyzer would flag a missing
    // arm.
    test('sealed pattern-match is exhaustive over three variants', () {
      String describe(StorageException e) => switch (e) {
        StorageTransientException() => 'transient',
        StoragePermanentException() => 'permanent',
        StorageCorruptException() => 'corrupt',
      };

      final cause = StateError('x');
      final stack = StackTrace.current;

      expect(
        describe(StorageTransientException('t', cause, stack)),
        'transient',
      );
      expect(
        describe(StoragePermanentException('p', cause, stack)),
        'permanent',
      );
      expect(describe(StorageCorruptException('c', cause, stack)), 'corrupt');
    });

    // StorageException AND implements dart:core Exception.
    test('variants extend StorageException and implement Exception', () {
      final cause = StateError('x');
      final stack = StackTrace.current;

      expect(
        StorageTransientException('t', cause, stack),
        isA<StorageException>(),
      );
      expect(
        StoragePermanentException('p', cause, stack),
        isA<StorageException>(),
      );
      expect(
        StorageCorruptException('c', cause, stack),
        isA<StorageException>(),
      );

      expect(StorageTransientException('t', cause, stack), isA<Exception>());
      expect(StoragePermanentException('p', cause, stack), isA<Exception>());
      expect(StorageCorruptException('c', cause, stack), isA<Exception>());
    });

    // original cause and stackTrace passed to its constructor.
    test('StorageTransientException preserves cause and stackTrace', () {
      final cause = StateError('locked');
      final stack = StackTrace.current;
      final e = StorageTransientException('db is locked', cause, stack);

      expect(e.message, 'db is locked');
      expect(identical(e.cause, cause), isTrue);
      expect(identical(e.stackTrace, stack), isTrue);
    });

    // original cause and stackTrace passed to its constructor.
    test('StoragePermanentException preserves cause and stackTrace', () {
      final cause = ArgumentError('bad path');
      final stack = StackTrace.current;
      final e = StoragePermanentException('bad arg', cause, stack);

      expect(e.message, 'bad arg');
      expect(identical(e.cause, cause), isTrue);
      expect(identical(e.stackTrace, stack), isTrue);
    });

    // original cause and stackTrace passed to its constructor.
    test('StorageCorruptException preserves cause and stackTrace', () {
      const cause = FormatException('bad JSON');
      final stack = StackTrace.current;
      final e = StorageCorruptException('decode failed', cause, stack);

      expect(e.message, 'decode failed');
      expect(identical(e.cause, cause), isTrue);
      expect(identical(e.stackTrace, stack), isTrue);
    });

    test(
      'toString() includes runtimeType, message, and cause for diagnostics',
      () {
        final e = StorageCorruptException(
          'bad event row',
          const FormatException('bad JSON'),
          StackTrace.current,
        );
        final s = e.toString();
        expect(s, contains('StorageCorruptException'));
        expect(s, contains('bad event row'));
        expect(s, contains('FormatException'));
      },
    );
  });
}
