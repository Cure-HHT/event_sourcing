// Verifies: EVS-PRD-destinations/A+F
// exercises DestinationRegistry:
// configuring destinations (add, all, byId — A) and dynamic registration
// after first read (F).
import 'dart:typed_data';

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kDestinationRegisteredEntryType,
        kDestinationRegisteredEventType,
        kSecurityFindingEntryType;
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/registry_with_audit.dart';

const Initiator _testInit = AutomationInitiator(service: 'test-bootstrap');

class _StubDestination extends Destination {
  _StubDestination(this._id, {SubscriptionFilter? filter})
    : _filter = filter ?? const SubscriptionFilter();

  final String _id;
  final SubscriptionFilter _filter;

  @override
  String get id => _id;

  @override
  SubscriptionFilter get filter => _filter;

  @override
  String get wireFormat => 'stub-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async => WirePayload(
    bytes: Uint8List.fromList(batch.first.eventId.codeUnits),
    contentType: 'text/plain',
    transformVersion: 'stub-v1',
  );

  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}

/// A destination that serializes natively and implements no pull.
class _NativeWithoutPull extends _StubDestination {
  _NativeWithoutPull(super.id);

  @override
  bool get serializesNatively => true;

  @override
  String get wireFormat => 'esd/batch@3';
}

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

void main() {
  group('DestinationRegistry (instance-based', () {
    late SembastBackend backend;
    late DestinationRegistry registry;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('registry-$dbCounter.db');
      final deps = await buildAuditedRegistryDeps(backend);
      registry = DestinationRegistry(eventStore: deps.eventStore);
    });

    tearDown(() async {
      await backend.close();
    });

    test('addDestination adds a destination and all() returns it', () async {
      final d = _StubDestination('primary');
      await registry.addDestination(d, initiator: _testInit);
      expect(registry.all(), contains(d));
    });

    // Verifies: EVS-DEV-view-convergence/E
    // Contrast with EVS-PRD-materializer/I, which covers an always-stored
    // event's copy passing over a fold failure instead of throwing.
    test('addDestination throws, storing nothing, when a registered view '
        'cannot key the destination_registered event', () async {
      final localBackend = await _openBackend('registry-local-op-throws.db');
      const view = 'unkeyable_destination_registrations';
      const spec = TableProjectionSpec(
        viewName: view,
        interest: SubscriptionFilter(
          entryTypes: <String>{kDestinationRegisteredEntryType},
          includeSystemEvents: true,
        ),
        insertEventTypes: <String>{kDestinationRegisteredEventType},
        removeEventTypes: <String>{},
        rowKey: CompositeKey(<String>['data.no_such_field']),
        rowData: WholePayload(),
      );
      final deps = await buildAuditedRegistryDeps(
        localBackend,
        projections: ProjectionRegistry()..register(spec),
      );
      final localRegistry = DestinationRegistry(eventStore: deps.eventStore);
      final d = _StubDestination('primary');

      await expectLater(
        localRegistry.addDestination(d, initiator: _testInit),
        throwsA(isA<StateError>()),
        reason:
            "destination_registered is a public local operation's own "
            'append; a fold failure fails it to its caller with nothing '
            'stored, unlike an always-stored record',
      );
      expect(localRegistry.all(), isEmpty);
      expect(
        await deps.eventStore.reader.findAllEvents(
          entryType: kDestinationRegisteredEntryType,
        ),
        isEmpty,
      );
      expect(
        await deps.eventStore.reader.findAllEvents(
          entryType: kSecurityFindingEntryType,
        ),
        isEmpty,
        reason:
            'nothing is recorded: the failure fails to the caller instead '
            'of being passed over and recorded as a fold_failed finding',
      );
      await localBackend.close();
    });

    // addDestination throws ArgumentError.
    test('addDestination with duplicate id throws ArgumentError', () async {
      await registry.addDestination(
        _StubDestination('primary'),
        initiator: _testInit,
      );
      await expectLater(
        registry.addDestination(
          _StubDestination('primary'),
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
    });

    // all() does not freeze the registry on first read. Subsequent
    // addDestination after all() succeeds.
    test('first all() read does NOT freeze the registry; a '
        'subsequent addDestination succeeds', () async {
      await registry.addDestination(
        _StubDestination('primary'),
        initiator: _testInit,
      );
      registry.all();
      await registry.addDestination(
        _StubDestination('secondary'),
        initiator: _testInit,
      );
      expect(registry.all().map((d) => d.id), ['primary', 'secondary']);
    });

    // all() returns an unmodifiable view so callers cannot mutate the
    // registry by mutating the returned list.
    test('all() returns an unmodifiable view', () async {
      await registry.addDestination(
        _StubDestination('primary'),
        initiator: _testInit,
      );
      final dests = registry.all();
      expect(
        () => dests.add(_StubDestination('other')),
        throwsUnsupportedError,
      );
    });

    // Verifies: EVS-DEV-delivery-channel/B
    // a destination that serializes natively and implements no pull is
    //   refused before anything is written: no schedule, no sender channel
    //   record, no registration event and no registry check record.
    test('a native destination without a pull is refused and nothing is '
        'written', () async {
      final eventsBefore = (await backend.findAllEvents()).length;
      await expectLater(
        registry.addDestination(
          _NativeWithoutPull('native'),
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
      expect(await backend.readSchedule('native'), isNull);
      expect(await backend.findAllEvents(), hasLength(eventsBefore));
      await backend.transaction((txn) async {
        expect(await backend.readRegistryCheckTxn(txn), isNull);
        expect(await backend.readSenderChannelRecordTxn(txn, 'native'), isNull);
      });
      expect(registry.byId('native'), isNull);
      // The refusal holds nothing: the id registers once it has a pull.
      await registry.addDestination(
        _StubDestination('native'),
        initiator: _testInit,
      );
      expect(registry.byId('native'), isNotNull);
    });

    // byId returns null for unknown ids, the destination for known ids.
    test('byId returns null for unknown ids', () async {
      expect(registry.byId('ghost'), isNull);
      final d = _StubDestination('primary');
      await registry.addDestination(d, initiator: _testInit);
      expect(registry.byId('primary'), same(d));
    });
  });
}
