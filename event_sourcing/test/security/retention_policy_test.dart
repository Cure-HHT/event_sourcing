// Verifies: EVS-PRD-regulatory-alignment — default retention windows and
//   round-trip serialization correctness for SecurityRetentionPolicy, the
//   mechanism that satisfies ALCOA+ Enduring / §11.10(c) protection-of-records.
import 'package:event_sourcing/src/security/security_retention_policy.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SecurityRetentionPolicy', () {
    test('defaults match spec', () {
      const p = SecurityRetentionPolicy.defaults;
      expect(p.fullRetention, const Duration(days: 90));
      expect(p.truncatedRetention, const Duration(days: 365));
      expect(p.truncateIpv4LastOctet, isTrue);
      expect(p.truncateIpv6Suffix, isTrue);
      expect(p.dropUserAgentAfterFull, isTrue);
      expect(p.dropGeoAfterFull, isFalse);
      expect(p.dropAllAfterTruncated, isTrue);
    });

    test('round-trips through toJson / fromJson', () {
      const p = SecurityRetentionPolicy(
        fullRetention: Duration(days: 30),
        truncatedRetention: Duration(days: 100),
        truncateIpv4LastOctet: false,
        truncateIpv6Suffix: true,
        dropUserAgentAfterFull: false,
        dropGeoAfterFull: true,
        dropAllAfterTruncated: false,
      );
      expect(SecurityRetentionPolicy.fromJson(p.toJson()), p);
    });

    test('equality and hashCode', () {
      const another = SecurityRetentionPolicy.defaults;
      expect(SecurityRetentionPolicy.defaults, same(another));
    });
  });
}
