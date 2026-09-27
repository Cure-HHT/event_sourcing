// Verifies: EVS-DEV-severe-log-default/A
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  setUp(() {
    LibraryLogging.severeToStandardError = true;
  });
  tearDown(() {
    LibraryLogging.severeToStandardError = true;
  });

  test('a severe drain record reaches the stderr sink', () {
    final lines = <String>[];
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () =>
          libraryLog('drain', 'a drain failure', level: LibraryLogLevel.severe),
    );
    expect(lines.any((l) => l.contains('a drain failure')), isTrue);
  });

  test('a severe sync_cycle record reaches the stderr sink', () {
    final lines = <String>[];
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () => libraryLog(
        'sync_cycle',
        'a fill failure',
        level: LibraryLogLevel.severe,
      ),
    );
    expect(lines.any((l) => l.contains('a fill failure')), isTrue);
  });

  test('a warning drain record does not reach the stderr sink', () {
    final lines = <String>[];
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () => libraryLog(
        'drain',
        'a drain warning',
        level: LibraryLogLevel.warning,
      ),
    );
    expect(lines, isEmpty);
  });

  test('a severe record from a non-fill/drain component does not reach the '
      'stderr sink', () {
    final lines = <String>[];
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () => libraryLog(
        'event_store',
        'an unrelated severe record',
        level: LibraryLogLevel.severe,
      ),
    );
    expect(lines, isEmpty);
  });

  test('a severe record with a stack trace carries it on the written line', () {
    final lines = <String>[];
    final trace = StackTrace.current;
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () => libraryLog(
        'drain',
        'a drain failure with a trace',
        level: LibraryLogLevel.severe,
        error: StateError('boom'),
        stackTrace: trace,
      ),
    );
    expect(lines, hasLength(1));
    expect(lines.single, contains('a drain failure with a trace'));
    expect(lines.single, contains(trace.toString()));
  });

  test('with the default turned off, nothing is written for a severe drain '
      'record', () {
    LibraryLogging.severeToStandardError = false;
    final lines = <String>[];
    runWithDeliveryTestHooks(
      DeliveryTestHooks(severeLogSink: lines.add),
      () => libraryLog(
        'drain',
        'a drain failure while off',
        level: LibraryLogLevel.severe,
      ),
    );
    expect(lines, isEmpty);
  });
}
