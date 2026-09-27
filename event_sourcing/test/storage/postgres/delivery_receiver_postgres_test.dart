// Runs the scenarios of delivery_receiver_conformance.dart on Postgres, and
// races two presentations of one delivery through two event stores over
// one database; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/delivery_receiver_conformance.dart.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../../test_support/delivery_receiver_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'delrecv');
  if (pg != null) tearDownAll(pg.drop);
  runDeliveryReceiverScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );

  // Verifies: EVS-DEV-delivery-receiver/B
  // Verifies: EVS-PRD-delivery-channel/C
  test(
    'two instances presented the same next delivery at once accept it once '
    'and acknowledge the other presentation as represented',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () async {
      final db = (await PostgresScenarioDatabase.fresh(pg))!;
      final first = await openReceiverStore(db);
      final second = await openReceiverStore(db);
      addTearDown(() async {
        await second.close();
        await first.close();
        await db.close();
      });
      final senders = <String>{deliveryChannel().senderDatabaseId};
      var previous = sealedDelivery();
      await first.receiverEndpoint.accept(
        previous.encode(),
        senderDatabaseIds: senders,
      );

      for (var number = 2; number <= 11; number++) {
        final next = sealedDelivery(
          number: number,
          link: previous.deliveryHash,
        );
        final bytes = next.encode();
        final answers = await Future.wait(<Future<ReceiverResponse>>[
          first.receiverEndpoint.accept(bytes, senderDatabaseIds: senders),
          second.receiverEndpoint.accept(bytes, senderDatabaseIds: senders),
        ]);
        final outcomes = <AcknowledgementOutcome>[
          for (final a in answers) (a as ReceiverAcknowledgement).outcome,
        ]..sort((a, b) => a.index.compareTo(b.index));
        expect(outcomes, <AcknowledgementOutcome>[
          AcknowledgementOutcome.accepted,
          AcknowledgementOutcome.represented,
        ], reason: 'delivery $number');
        for (final a in answers) {
          expect(a.record, recordAfter(next));
        }
        previous = next;
      }

      final audits = await authoredDeliveryAudits(first);
      expect(
        <Object?>[for (final a in audits) a.data['delivery_number']],
        <int>[for (var n = 1; n <= 11; n++) n],
        reason: 'exactly one audit per delivery',
      );
    },
  );
}
