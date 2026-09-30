import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('EntryTypeDefinition', () {
    test('constructs with all required fields; getters round-trip', () {
      const def = EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Nosebleed',
      );

      expect(def.id, 'epistaxis_event');
      expect(def.registeredVersion, const EntryTypeVersion(1, 0));
      expect(def.name, 'Nosebleed');
      expect(def.declarations, isEmpty);
    });
  });

  group('per-event-type declarations', () {
    const noted = EntryTypeDefinition(
      id: 'scored_note',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Scored Note',
      declarations: <EventTypeDeclaration>[
        EventTypeDeclaration(
          eventType: 'score_recorded',
          kind: CausalKind.annotation,
          eligible: false,
        ),
        EventTypeDeclaration(
          eventType: 'draft_saved',
          kind: CausalKind.version,
          eligible: false,
        ),
      ],
    );

    // Verifies: EVS-DEV-causal-parents/C
    test('an event type the definition does not declare is an eligible '
        'version', () {
      final declaration = noted.declarationFor('finalized');
      expect(declaration.eventType, 'finalized');
      expect(declaration.kind, CausalKind.version);
      expect(declaration.eligible, isTrue);

      const bare = EntryTypeDefinition(
        id: 'bare',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Bare',
      );
      expect(bare.declarationFor('anything').kind, CausalKind.version);
      expect(bare.declarationFor('anything').eligible, isTrue);
    });

    // Verifies: EVS-DEV-causal-parents/C
    test('a declared event type yields its declared kind and eligibility', () {
      final score = noted.declarationFor('score_recorded');
      expect(score.kind, CausalKind.annotation);
      expect(score.eligible, isFalse);

      final draft = noted.declarationFor('draft_saved');
      expect(draft.kind, CausalKind.version);
      expect(draft.eligible, isFalse);
    });

    // Verifies: EVS-DEV-causal-parents/D
    test('a definition that declares one event type twice is refused', () {
      const twice = EntryTypeDefinition(
        id: 'twice',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Twice',
        declarations: <EventTypeDeclaration>[
          EventTypeDeclaration(
            eventType: 'finalized',
            kind: CausalKind.version,
            eligible: true,
          ),
          EventTypeDeclaration(
            eventType: 'finalized',
            kind: CausalKind.annotation,
            eligible: false,
          ),
        ],
      );
      final registry = EntryTypeRegistry();
      expect(
        () => registry.register(twice),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message,
            'message',
            contains('finalized'),
          ),
        ),
      );
      expect(registry.isRegistered('twice'), isFalse);

      // A definition declaring each event type once registers.
      registry.register(noted);
      expect(registry.byId('scored_note'), same(noted));
    });
  });
}
