import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

Map<String, Object?> _ref(String id, [String? hash]) => <String, Object?>{
  'event_id': id,
  'event_hash': hash ?? 'hash-$id',
};

Map<String, Object?> _valid({
  Object? kind = 'version',
  Object? eligible = true,
  Object? parents,
}) => <String, Object?>{
  'kind': kind,
  'eligible': eligible,
  'parents': parents ?? <Object?>[_ref('a'), _ref('b')],
};

Matcher _refusedNaming(String field) => throwsA(
  isA<FormatException>().having((e) => e.message, 'message', contains(field)),
);

void main() {
  group('CausalRecord.fromJson accepts the causal shape', () {
    // Verifies: EVS-DEV-causal-parents/A
    test('a version with ascending parents', () {
      final record = CausalRecord.fromJson(_valid());
      expect(record.kind, CausalKind.version);
      expect(record.eligible, isTrue);
      expect(record.parents.map((p) => p.eventId), <String>['a', 'b']);
      expect(record.parents.first.eventHash, 'hash-a');
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('an ineligible annotation with no parents', () {
      final record = CausalRecord.fromJson(
        _valid(kind: 'annotation', eligible: false, parents: <Object?>[]),
      );
      expect(record.kind, CausalKind.annotation);
      expect(record.eligible, isFalse);
      expect(record.parents, isEmpty);
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('toJson emits the decoded map verbatim, holding exactly kind, '
        'eligible and parents', () {
      final json = _valid();
      final record = CausalRecord.fromJson(json);
      expect(record.toJson(), json);

      final built = CausalRecord(
        kind: CausalKind.annotation,
        eligible: false,
        parents: const <CausalRef>[CausalRef(eventId: 'a', eventHash: 'h')],
      );
      expect(built.toJson(), <String, Object?>{
        'kind': 'annotation',
        'eligible': false,
        'parents': <Object?>[
          <String, Object?>{'event_id': 'a', 'event_hash': 'h'},
        ],
      });
      expect(
        built.toJson().keys,
        unorderedEquals(<String>['kind', 'eligible', 'parents']),
      );
      expect(
        record.toJson().keys,
        unorderedEquals(<String>['kind', 'eligible', 'parents']),
      );
      expect(CausalRecord.fromJson(built.toJson()), built);
    });
  });

  group('CausalRecord.fromJson refuses, naming the field', () {
    // Verifies: EVS-DEV-causal-parents/A
    test('a value that is not an object', () {
      expect(() => CausalRecord.fromJson('x'), _refusedNaming('causal'));
      expect(() => CausalRecord.fromJson(null), _refusedNaming('causal'));
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('a missing or extra key', () {
      expect(
        () => CausalRecord.fromJson(_valid()..remove('parents')),
        _refusedNaming('causal.parents'),
      );
      expect(
        () => CausalRecord.fromJson(_valid()..remove('kind')),
        _refusedNaming('causal.kind'),
      );
      expect(
        () => CausalRecord.fromJson(_valid()..['extra'] = 1),
        _refusedNaming('causal.extra'),
      );
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('a wrong kind or eligible', () {
      expect(
        () => CausalRecord.fromJson(_valid(kind: 'draft')),
        _refusedNaming('causal.kind'),
      );
      expect(
        () => CausalRecord.fromJson(_valid(kind: 1)),
        _refusedNaming('causal.kind'),
      );
      expect(
        () => CausalRecord.fromJson(_valid(eligible: 'true')),
        _refusedNaming('causal.eligible'),
      );
      expect(
        () => CausalRecord.fromJson(_valid(eligible: null)),
        _refusedNaming('causal.eligible'),
      );
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('parents that are not a list of exact references', () {
      expect(
        () => CausalRecord.fromJson(_valid()..['parents'] = null),
        _refusedNaming('causal.parents'),
      );
      expect(
        () => CausalRecord.fromJson(_valid(parents: <Object?>['a'])),
        _refusedNaming('causal.parents[0]'),
      );
      expect(
        () => CausalRecord.fromJson(
          _valid(
            parents: <Object?>[
              <String, Object?>{'event_id': 'a'},
            ],
          ),
        ),
        _refusedNaming('causal.parents[0].event_hash'),
      );
      expect(
        () => CausalRecord.fromJson(
          _valid(
            parents: <Object?>[
              _ref('a'),
              <String, Object?>{...(_ref('b')), 'extra': 1},
            ],
          ),
        ),
        _refusedNaming('causal.parents[1].extra'),
      );
      expect(
        () => CausalRecord.fromJson(
          _valid(
            parents: <Object?>[
              <String, Object?>{'event_id': 7, 'event_hash': 'h'},
            ],
          ),
        ),
        _refusedNaming('causal.parents[0].event_id'),
      );
      expect(
        () => CausalRecord.fromJson(
          _valid(
            parents: <Object?>[
              <String, Object?>{'event_id': 'a', 'event_hash': ''},
            ],
          ),
        ),
        _refusedNaming('causal.parents[0].event_hash'),
      );
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('parents not in ascending event_id order', () {
      expect(
        () => CausalRecord.fromJson(
          _valid(parents: <Object?>[_ref('b'), _ref('a')]),
        ),
        _refusedNaming('causal.parents[1]'),
      );
      expect(
        () => CausalRecord.fromJson(
          _valid(parents: <Object?>[_ref('a'), _ref('a', 'other')]),
        ),
        _refusedNaming('causal.parents[1]'),
      );
    });

    // Verifies: EVS-DEV-causal-parents/A
    test('a reconciles key, whatever its value', () {
      for (final value in <Object?>[
        null,
        <Object?>[],
        <Object?>[_ref('s1')],
      ]) {
        expect(
          () => CausalRecord.fromJson(_valid()..['reconciles'] = value),
          _refusedNaming('causal.reconciles'),
        );
      }
    });
  });

  group('the CausalRecord constructor', () {
    // Verifies: EVS-DEV-causal-parents/A
    test('refuses parents out of order', () {
      expect(
        () => CausalRecord(
          kind: CausalKind.version,
          eligible: true,
          parents: const <CausalRef>[
            CausalRef(eventId: 'b', eventHash: 'h'),
            CausalRef(eventId: 'a', eventHash: 'h'),
          ],
        ),
        throwsArgumentError,
      );
    });
  });
}
