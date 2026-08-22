// Verifies: EVS-PRD-action-dispatch/C
// (denial event factories produce correctly-shaped EventDrafts for every failure stage)
// Verifies: EVS-PRD-action-dispatch/B
// (one factory per stage maps directly to the B-stage taxonomy)

import 'package:event_sourcing/src/actions/authorization_decision.dart'
    show DenyReason;
import 'package:event_sourcing/src/actions/denial_events.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:test/test.dart';

void main() {
  group('denial event factories', () {
    test('unknownAction draft has correct shape', () {
      final draft = denialUnknownAction(
        invocationId: 'inv-1',
        requestedName: 'foo',
        actionInvocationMetadata: <String, dynamic>{'request_id': 'r-1'},
      );
      expect(draft.aggregateType, 'action_attempt');
      expect(draft.aggregateId, 'inv-1');
      expect(draft.entryType, 'action_denial');
      expect(draft.eventType, 'unknown_action');
      expect(draft.data['requested_name'], 'foo');
      expect(draft.metadata?['request_id'], 'r-1');
    });

    test('parseDenied includes sanitized error message', () {
      final draft = denialParseDenied(
        invocationId: 'inv-1',
        actionName: 'invite_user',
        error: ArgumentError(
          'email required at /home/user/secret/file.dart:42',
        ),
      );
      expect(draft.eventType, 'parse_denied');
      expect(draft.data['error_class'], 'ArgumentError');
      expect(
        draft.data['error_message_sanitized'],
        isNot(contains('/home/user/secret')),
      );
    });

    test('validationDenied carries error class', () {
      final draft = denialValidationDenied(
        invocationId: 'inv-1',
        actionName: 'invite_user',
        error: StateError('email malformed'),
      );
      expect(draft.eventType, 'validation_denied');
      expect(draft.data['error_class'], 'StateError');
      expect(draft.data['action_name'], 'invite_user');
    });

    test('validationDenied carries optional fieldPath', () {
      final draft = denialValidationDenied(
        invocationId: 'inv-1',
        actionName: 'invite_user',
        error: StateError('bad'),
        fieldPath: 'email',
      );
      expect(draft.data['field_path'], 'email');
    });

    test('authorizationDenied includes permission and active role', () {
      final draft = denialAuthorizationDenied(
        invocationId: 'inv-1',
        actionName: 'user.delete',
        permission: const Permission('user.delete'),
        principalActiveRole: 'Investigator',
      );
      expect(draft.eventType, 'authorization_denied');
      expect(draft.data['permission_denied'], 'user.delete');
      expect(draft.data['principal_active_role'], 'Investigator');
    });

    test('authorizationDenied without active role omits the field', () {
      final draft = denialAuthorizationDenied(
        invocationId: 'inv-1',
        actionName: 'user.delete',
        permission: const Permission('user.delete'),
      );
      expect(draft.data.containsKey('principal_active_role'), isFalse);
    });

    test('authorizationDenied with denyReason serializes enum name', () {
      final draft = denialAuthorizationDenied(
        invocationId: 'inv-1',
        actionName: 'user.delete',
        permission: const Permission('user.delete'),
        denyReason: DenyReason.scopeUnresolvable,
      );
      expect(draft.data['deny_reason'], 'scopeUnresolvable');
    });

    test('authorizationDenied without denyReason omits deny_reason field', () {
      final draft = denialAuthorizationDenied(
        invocationId: 'inv-1',
        actionName: 'user.delete',
        permission: const Permission('user.delete'),
      );
      expect(draft.data.containsKey('deny_reason'), isFalse);
    });

    test('idempotencyMismatch carries hashes, action_name, and key', () {
      // Verifies: EVS-PRD-action-dispatch/E — denial payload carries
      // the hashes but never the raw inputs themselves.
      final draft = denialIdempotencyMismatch(
        invocationId: 'inv-mm',
        actionName: 'submit_note',
        idempotencyKey: 'k1',
        cachedRawInputHash:
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        submittedRawInputHash:
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        actionInvocationMetadata: <String, dynamic>{'request_id': 'r-mm'},
      );
      expect(draft.aggregateType, 'action_attempt');
      expect(draft.entryType, 'action_denial');
      expect(draft.eventType, 'idempotency_mismatch');
      expect(draft.aggregateId, 'inv-mm');
      expect(draft.data['action_name'], 'submit_note');
      expect(draft.data['idempotency_key'], 'k1');
      expect(
        draft.data['cached_raw_input_hash'],
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      );
      expect(
        draft.data['submitted_raw_input_hash'],
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      );
      expect(draft.metadata?['request_id'], 'r-mm');
    });

    test('executionFailed carries sanitized error', () {
      final draft = denialExecutionFailed(
        invocationId: 'inv-1',
        actionName: 'invite_user',
        error: StateError('boom'),
      );
      expect(draft.eventType, 'execution_failed');
      expect(draft.data['error_class'], 'StateError');
    });

    test('sanitization strips stack-trace markers', () {
      final draft = denialExecutionFailed(
        invocationId: 'inv-1',
        actionName: 'a',
        error: StateError(
          'boom\n#0 main (file:///home/me/foo.dart:10:5)\n#1 ...',
        ),
      );
      final msg = draft.data['error_message_sanitized'] as String;
      expect(msg, isNot(contains('#0 main')));
      expect(msg, isNot(contains('file:///')));
    });

    test('sanitization strips Windows paths', () {
      final draft = denialExecutionFailed(
        invocationId: 'inv-1',
        actionName: 'a',
        error: StateError(r'failed at C:\Users\me\code\foo.dart:99'),
      );
      final msg = draft.data['error_message_sanitized'] as String;
      expect(msg, isNot(contains(r'C:\Users')));
    });

    test('every denial type uses aggregateType=action_attempt', () {
      final all = <String>{
        denialUnknownAction(
          invocationId: 'i',
          requestedName: 'x',
        ).aggregateType,
        denialParseDenied(
          invocationId: 'i',
          actionName: 'a',
          error: 'e',
        ).aggregateType,
        denialValidationDenied(
          invocationId: 'i',
          actionName: 'a',
          error: 'e',
        ).aggregateType,
        denialAuthorizationDenied(
          invocationId: 'i',
          actionName: 'a',
          permission: const Permission('p'),
        ).aggregateType,
        denialExecutionFailed(
          invocationId: 'i',
          actionName: 'a',
          error: 'e',
        ).aggregateType,
        denialIdempotencyMismatch(
          invocationId: 'i',
          actionName: 'a',
          idempotencyKey: 'k',
          cachedRawInputHash: 'h1',
          submittedRawInputHash: 'h2',
        ).aggregateType,
      };
      expect(all, {'action_attempt'});
    });
  });
}
