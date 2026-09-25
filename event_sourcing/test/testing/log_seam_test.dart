// The log-observing seam only observes: a seam that throws does not reach
// the code that logged, which carries on as if no seam were installed.
// Library log lines also reach the `package:logging` logger of the same
// name, an observation route that is not a test seam.

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:logging/logging.dart';

void main() {
  test('an onLog seam that throws does not reach the code that logged', () {
    var observed = 0;
    var carriedOn = false;
    runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onLog: (record) {
          observed += 1;
          throw StateError('observer failure');
        },
      ),
      () {
        libraryLog('probe', 'first', level: LibraryLogLevel.warning);
        libraryLog('probe', 'second', level: LibraryLogLevel.severe);
        carriedOn = true;
      },
    );
    expect(observed, 2);
    expect(carriedOn, isTrue);
  });

  test('a library log line reaches the package:logging logger of its '
      'name', () async {
    final records = <LogRecord>[];
    final sub = Logger.root.onRecord.listen(records.add);
    addTearDown(sub.cancel);

    libraryLog('probe', 'visible', level: LibraryLogLevel.warning);

    final mine = records.where((r) => r.loggerName == 'event_sourcing.probe');
    expect(mine, hasLength(1));
    expect(mine.single.level, Level.WARNING);
    expect(mine.single.message, 'visible');
  });
}
