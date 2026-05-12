// Verifies: EVS-PRD-event-log/C — Source carries hopId + identifier that
//   together identify the authority whose events must stay ordered within an
//   aggregate.
// Verifies: EVS-PRD-portability/C — pure Dart value; equality and hashCode
//   are consistent across all Dart-supported runtimes.
import 'package:event_sourcing/src/storage/source.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Source', () {
    test('construction carries three fields (no userId)', () {
      const s = Source(
        hopId: 'mobile-device',
        identifier: 'dev-1',
        softwareVersion: 'clinical_diary@1.2.3+4',
      );
      expect(s.hopId, 'mobile-device');
      expect(s.identifier, 'dev-1');
      expect(s.softwareVersion, 'clinical_diary@1.2.3+4');
    });

    test('equality and hashCode', () {
      const a = Source(hopId: 'h', identifier: 'i', softwareVersion: 'v');
      const b = Source(hopId: 'h', identifier: 'i', softwareVersion: 'v');
      const c = Source(hopId: 'x', identifier: 'i', softwareVersion: 'v');
      expect(a, b);
      expect(a.hashCode, b.hashCode);
      expect(a, isNot(c));
    });

    test('hopId accepts well-known values', () {
      const m = Source(
        hopId: 'mobile-device',
        identifier: 'd',
        softwareVersion: 'v',
      );
      const p = Source(
        hopId: 'portal-server',
        identifier: 'h',
        softwareVersion: 'v',
      );
      expect(m.hopId, 'mobile-device');
      expect(p.hopId, 'portal-server');
    });

    test('softwareVersion is accepted without runtime validation', () {
      const s = Source(
        hopId: 'mobile-device',
        identifier: 'd',
        softwareVersion: 'anything-goes',
      );
      expect(s.softwareVersion, 'anything-goes');
    });
  });
}
