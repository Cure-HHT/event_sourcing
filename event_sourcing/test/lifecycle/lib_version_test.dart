// Verifies: EVS-DEV-event-store-open/B/C/D
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('LibVersion', () {
    test('current version is a non-empty string', () {
      expect(LibVersion.current, isNotEmpty);
    });

    test('compare returns expected ordering', () {
      expect(LibVersion.compare('0.4.0', '0.4.1'), lessThan(0));
      expect(LibVersion.compare('0.4.1', '0.4.0'), greaterThan(0));
      expect(LibVersion.compare('0.4.0', '0.4.0'), 0);
      expect(LibVersion.compare('0.10.0', '0.9.0'), greaterThan(0));
    });

    test('event type ids are stable strings', () {
      expect(LibVersionEvents.initialized, 'lib_version_initialized');
      expect(LibVersionEvents.changed, 'lib_version_changed');
    });
  });
}
