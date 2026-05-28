// Verifies: EVS-PRD-cross-process-event-transport/A — round-trip codecs
//   for the WS control plane messages (auth, subscribe, unsubscribe,
//   auth_ok, subscription_denied, stale_data, error).
// Verifies: EVS-PRD-cross-process-event-transport/B — subscriptionId
//   on every applicable server-to-client envelope.
// Verifies: EVS-PRD-cross-process-event-transport/D — SubscribeMsg
//   carries the client-chosen subscriptionId.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/subscription_messages.dart';

void main() {
  test('round-trips AuthMsg', () {
    const original = AuthMsg(credential: 'opaque-token');
    final j = SubscriptionMessages.encodeClient(original);
    expect(j, {'type': 'auth', 'credential': 'opaque-token'});
    final d = SubscriptionMessages.decodeClient(j) as AuthMsg;
    expect(d.credential, 'opaque-token');
  });

  test('round-trips SubscribeMsg minimal', () {
    const original = SubscribeMsg(
      subscriptionId: 'sub-1',
      viewName: 'notes_today',
    );
    final j = SubscriptionMessages.encodeClient(original);
    final d = SubscriptionMessages.decodeClient(j) as SubscribeMsg;
    expect(d.subscriptionId, 'sub-1');
    expect(d.viewName, 'notes_today');
    expect(d.filter, isNull);
    expect(d.aggregates, isNull);
  });

  test('round-trips SubscribeMsg with filter and aggregates', () {
    const original = SubscribeMsg(
      subscriptionId: 'sub-2',
      viewName: 'patient_files',
      filter: SubscriptionFilter(entryTypes: {'note'}),
      aggregates: {'p-1', 'p-2'},
    );
    final j = SubscriptionMessages.encodeClient(original);
    final d = SubscriptionMessages.decodeClient(j) as SubscribeMsg;
    expect(d.filter?.entryTypes, {'note'});
    expect(d.aggregates, {'p-1', 'p-2'});
  });

  test('round-trips UnsubscribeMsg', () {
    const original = UnsubscribeMsg(subscriptionId: 'sub-1');
    final j = SubscriptionMessages.encodeClient(original);
    expect(j, {'type': 'unsubscribe', 'subscriptionId': 'sub-1'});
  });

  test('round-trips AuthOkMsg', () {
    const original = AuthOkMsg(principalId: 'u-1');
    final j = SubscriptionMessages.encodeServer(original);
    expect(j, {'type': 'auth_ok', 'principalId': 'u-1'});
  });

  test('round-trips SubscriptionDeniedMsg', () {
    const original = SubscriptionDeniedMsg(
      subscriptionId: 'sub-1',
      reason: SubscriptionDenyReason.viewPermissionDenied,
    );
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'subscription_denied');
    expect(j['reason'], 'view_permission_denied');
  });

  test('round-trips ErrorMsg', () {
    const original = ErrorMsg(
      code: WireErrorCode.protocolError,
      message: 'bad json',
    );
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'error');
    expect(j['code'], 'protocol_error');
  });

  test('round-trips StaleDataMsg with reason', () {
    const original = StaleDataMsg(reason: StaleDataReason.roleAssigned);
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'stale_data');
    expect(j['reason'], 'role_assigned');
  });

  test('round-trips StaleDataMsg without reason', () {
    const original = StaleDataMsg();
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'stale_data');
    expect(j.containsKey('reason'), isFalse);
  });
}
