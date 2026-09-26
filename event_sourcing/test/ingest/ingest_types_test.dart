// Verifies: EVS-PRD-ingest/F
// IngestOutcome distinguishes a stored event, a stored event with findings,
//   a held duplicate and a record kept in a finding, confirming the
//   idempotency semantics contract
// Verifies: EVS-DEV-chain-verification/I
// a chain verification verdict is valid exactly when it lists no finding

import 'package:event_sourcing/src/ingest/ingest_result.dart';
import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:event_sourcing/src/verification/chain_verification_verdict.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('IngestOutcome', () {
    test('enum has the four outcomes of a received record', () {
      expect(IngestOutcome.values, <IngestOutcome>[
        IngestOutcome.ingested,
        IngestOutcome.ingestedWithFinding,
        IngestOutcome.duplicate,
        IngestOutcome.keptInFinding,
      ]);
    });
  });

  group('ChainVerificationVerdict', () {
    test('is valid exactly when it lists no finding', () {
      const valid = ChainVerificationVerdict(
        from: 1,
        to: 4,
        findings: <ChainVerificationFinding>[],
        unresolvedPredecessors: 2,
      );
      expect(valid.isValid, isTrue);
      const invalid = ChainVerificationVerdict(
        from: 1,
        to: 4,
        findings: <ChainVerificationFinding>[
          ChainVerificationFinding(
            kind: FindingKind.sequenceMissing,
            evidence: <String, Object?>{'local_sequence_number': 3},
            aggregates: <String>[],
          ),
        ],
        unresolvedPredecessors: 0,
      );
      expect(invalid.isValid, isFalse);
    });
  });
}
