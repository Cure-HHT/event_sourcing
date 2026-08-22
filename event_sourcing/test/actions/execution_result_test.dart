// Verifies: EVS-PRD-action-dispatch/B
// (ExecutionResult carries the events list returned from the execute stage for Stage 8 persist)
// Verifies: EVS-PRD-action-dispatch/C
// (events in ExecutionResult are the payload of the recorded success outcome)

import 'package:event_sourcing/event_sourcing.dart'
    show EventDraft, SecurityDetails;
import 'package:event_sourcing/src/actions/execution_result.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('ExecutionResult', () {
    test('holds result + events', () {
      const r = ExecutionResult<int>(result: 42, events: <EventDraft>[]);
      expect(r.result, 42);
      expect(r.events, isEmpty);
      expect(r.securityDetailsOverride, isNull);
    });

    test('events list MAY be empty (no-op success)', () {
      const r = ExecutionResult<String>(result: 'ok', events: <EventDraft>[]);
      expect(r.events, isEmpty);
    });

    test('securityDetailsOverride is preserved when set', () {
      const sd = SecurityDetails(ipAddress: '10.0.0.1');
      const r = ExecutionResult<void>(
        result: null,
        events: <EventDraft>[],
        securityDetailsOverride: sd,
      );
      expect(r.securityDetailsOverride, isNotNull);
      expect(r.securityDetailsOverride?.ipAddress, '10.0.0.1');
    });
  });
}
