import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('LibVersion', () {
    // Verifies: EVS-DEV-event-store-open/D
    test('current version is a non-empty string, and the data format is '
        'exported beside it', () {
      expect(LibVersion.version, isNotEmpty);
      expect(LibVersion.dataFormat.major, greaterThanOrEqualTo(1));
    });

    test('compare returns expected ordering', () {
      expect(LibVersion.compare('0.4.0', '0.4.1'), lessThan(0));
      expect(LibVersion.compare('0.4.1', '0.4.0'), greaterThan(0));
      expect(LibVersion.compare('0.4.0', '0.4.0'), 0);
      expect(LibVersion.compare('0.10.0', '0.9.0'), greaterThan(0));
    });

    // Verifies: EVS-DEV-event-store-open/B+C
    test('event type ids are stable strings', () {
      expect(LibVersionEvents.initialized, 'lib_version_initialized');
      expect(LibVersionEvents.changed, 'lib_version_changed');
    });
  });
}
