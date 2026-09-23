// Verifies: EVS-DEV-destination-drain/F
// the declared configuration of a destination and its fingerprint: stable
//   across the insertion order of the filter's sets; different for a
//   change of any declared field (null and an empty set differ); equal for
//   a change of the hard-delete opt-in alone, and for a change of code the
//   library cannot read unless the configuration version changes.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

/// A destination whose every declared field is a constructor argument,
/// and whose transform tag stands for code the library cannot read.
class _Configured extends Destination {
  _Configured({
    this.id = 'x',
    this.wireFormat = 'w1',
    this.serializesNatively = false,
    this.maxAccumulateTime = Duration.zero,
    this.allowHardDelete = false,
    Set<String>? entryTypes = const <String>{'a', 'b'},
    Set<String>? eventTypes,
    Set<String>? aggregateTypes,
    bool includeSystemEvents = false,
    SubscriptionPredicate? predicate,
    this.transformTag = 't1',
  }) : filter = SubscriptionFilter(
         entryTypes: entryTypes,
         eventTypes: eventTypes,
         aggregateTypes: aggregateTypes,
         includeSystemEvents: includeSystemEvents,
         predicate: predicate,
       );

  @override
  final String id;
  @override
  final String wireFormat;
  @override
  final bool serializesNatively;
  @override
  final Duration maxAccumulateTime;
  @override
  final bool allowHardDelete;
  @override
  final SubscriptionFilter filter;
  final String transformTag;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      true;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw UnimplementedError(transformTag);

  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}

String _fp(Destination d, [String? version]) =>
    configurationFingerprint(declaredConfiguration(d, version));

bool _anything(StoredEvent e) => true;

void main() {
  test('the map carries the declared fields, sets sorted', () {
    final map = declaredConfiguration(
      _Configured(entryTypes: <String>{'b', 'a'}, eventTypes: <String>{'z'}),
      'build-1',
    );
    expect(map, <String, Object?>{
      'id': 'x',
      'wire_format': 'w1',
      'serializes_natively': false,
      'max_accumulate_time_micros': 0,
      'filter': <String, Object?>{
        'entry_types': <String>['a', 'b'],
        'event_types': <String>['z'],
        'aggregate_types': null,
        'include_system_events': false,
        'has_predicate': false,
      },
      'configuration_version': 'build-1',
    });
    expect(_fp(_Configured()), matches(RegExp(r'^[0-9a-f]{64}$')));
  });

  test('equal filters built in different insertion orders are equal', () {
    final first = _Configured(
      entryTypes: <String>{'a', 'b', 'c'},
      eventTypes: <String>{'x', 'y'},
      aggregateTypes: <String>{'p', 'q'},
    );
    final second = _Configured(
      entryTypes: <String>{'c', 'a', 'b'},
      eventTypes: <String>{'y', 'x'},
      aggregateTypes: <String>{'q', 'p'},
    );
    expect(_fp(first), _fp(second));
  });

  test('each declared field changes the fingerprint', () {
    final base = _Configured();
    final variants = <String, Destination>{
      'id': _Configured(id: 'y'),
      'wireFormat': _Configured(wireFormat: 'w2'),
      'serializesNatively': _Configured(serializesNatively: true),
      'maxAccumulateTime': _Configured(
        maxAccumulateTime: const Duration(seconds: 1),
      ),
      'entryTypes': _Configured(entryTypes: <String>{'a'}),
      'entryTypes null': _Configured(entryTypes: null),
      'entryTypes empty': _Configured(entryTypes: <String>{}),
      'eventTypes': _Configured(eventTypes: <String>{'e'}),
      'eventTypes empty': _Configured(eventTypes: <String>{}),
      'aggregateTypes': _Configured(aggregateTypes: <String>{'g'}),
      'aggregateTypes empty': _Configured(aggregateTypes: <String>{}),
      'includeSystemEvents': _Configured(includeSystemEvents: true),
      'predicate': _Configured(predicate: _anything),
    };
    for (final entry in variants.entries) {
      expect(_fp(entry.value), isNot(_fp(base)), reason: entry.key);
    }
    expect(_fp(base, 'v2'), isNot(_fp(base)), reason: 'configurationVersion');
    expect(
      _fp(_Configured(entryTypes: null)),
      isNot(_fp(_Configured(entryTypes: <String>{}))),
      reason: 'null and the empty set differ',
    );
  });

  test('the hard-delete opt-in and undeclared code are not inputs', () {
    expect(_fp(_Configured(allowHardDelete: true)), _fp(_Configured()));
    expect(_fp(_Configured(transformTag: 't2')), _fp(_Configured()));
    expect(
      _fp(_Configured(predicate: (e) => e.aggregateId.startsWith('a'))),
      _fp(_Configured(predicate: (e) => e.aggregateId.startsWith('b'))),
      reason: 'a different predicate body is undeclared code',
    );
    expect(
      _fp(_Configured(transformTag: 't2'), 'v2'),
      isNot(_fp(_Configured(), 'v1')),
      reason: 'a changed configuration version declares the change',
    );
  });
}
