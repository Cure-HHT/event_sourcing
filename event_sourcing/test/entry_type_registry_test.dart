import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

/// Minimal fixture: two distinct `EntryTypeDefinition`s with unique ids.
EntryTypeDefinition _defn(String id) => EntryTypeDefinition(
  id: id,
  registeredVersion: const EntryTypeVersion(1, 0),
  name: 'Defn $id',
);

void main() {
  group('EntryTypeRegistry', () {
    late EntryTypeRegistry registry;

    setUp(() {
      registry = EntryTypeRegistry();
    });

    // Registered definitions round-trip through byId by the id they were
    // registered under — the registry's primary lookup surface.
    test('register and byId round-trip', () {
      final definition = _defn('demo_note');
      registry.register(definition);
      expect(registry.byId('demo_note'), same(definition));
    });

    // byId returns null for unregistered ids so callers can distinguish
    // "unknown type" from "registered" with a null check.
    test('byId returns null for unknown id', () {
      expect(registry.byId('nope'), isNull);
    });

    // Silent shadowing would let an app declare two competing definitions
    // for the same entry type, and the later one would silently win —
    // changing the version stamped on every appended event of that type
    // mid-run. Loud failure at registration catches the config bug at boot.
    // Verifies: EVS-DEV-append-stamps-registered-version/D
    test('register of duplicate id throws ArgumentError', () {
      final original = _defn('demo_note');
      registry.register(original);
      expect(() => registry.register(_defn('demo_note')), throwsArgumentError);
      // State is unchanged by the throw: the original is still the
      // only entry and byId still returns it.
      expect(registry.all(), hasLength(1));
      expect(registry.byId('demo_note'), same(original));
    });

    // isRegistered returns true iff a definition is present under the id.
    // Convenience wrapper over byId != null for callers (notably
    // EntryService.record) that only need the yes/no.
    test('isRegistered matches byId presence', () {
      expect(registry.isRegistered('demo_note'), isFalse);
      registry.register(_defn('demo_note'));
      expect(registry.isRegistered('demo_note'), isTrue);
      expect(registry.isRegistered('other'), isFalse);
    });

    // all() returns every registered definition in registration order.
    // Ordering is incidental — the sole consumer, the bootstrap registry
    // snapshot, is hashed over canonical JSON, which sorts keys — so this
    // records current behaviour rather than a commitment.
    test('all() returns registered definitions in insertion order', () {
      final first = _defn('first');
      final second = _defn('second');
      final third = _defn('third');
      registry
        ..register(first)
        ..register(second)
        ..register(third);
      // orderedEquals uses == and preserves order; combined with
      // EntryTypeDefinition having no custom operator==, this asserts
      // both the ordering and identity-level reference equality.
      expect(registry.all(), orderedEquals([first, second, third]));
    });

    // The list returned by all() is unmodifiable so a caller cannot mutate
    // the registry's backing store by mutating the view.
    test('all() returns an unmodifiable list', () {
      registry.register(_defn('x'));
      final view = registry.all();
      expect(() => view.add(_defn('y')), throwsUnsupportedError);
      expect(view.clear, throwsUnsupportedError);
    });

    // A fresh registry holds no definitions.
    test('empty registry reports zero registrations', () {
      expect(registry.all(), isEmpty);
      expect(registry.isRegistered('any'), isFalse);
      expect(registry.byId('any'), isNull);
    });

    // Sealed like ProjectionRegistry and PromoterRegistry: three things
    // read the whole registry at EventStore.open (a fingerprint whose
    // interest names no entry type, the generation descriptor, the
    // registry audit), so a registration after open would silently change
    // them.
    // Verifies: EVS-DEV-version-compatibility/K (registry complete at open)
    test('register after seal throws and leaves the registry unchanged', () {
      final before = _defn('before_seal');
      registry
        ..register(before)
        ..seal();
      expect(registry.isSealed, isTrue);
      expect(() => registry.register(_defn('after_seal')), throwsArgumentError);
      expect(registry.all(), hasLength(1));
      expect(registry.byId('before_seal'), same(before));
      expect(registry.byId('after_seal'), isNull);
    });

    // A registration attempted after EventStore.open is refused and leaves
    // the registry as the boot read it.
    // Verifies: EVS-DEV-version-compatibility/K
    test('EventStore.open seals the entry-type registry it is given', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'entry-type-seal-open-${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final backend = SembastBackend(database: db);
      final registry = EntryTypeRegistry()..register(_defn('before_open'));

      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(
          backend,
          SembastSecurityContextStore(backend: backend),
        ),
        entryTypes: registry,
        source: const Source(
          hopId: 'test-server',
          identifier: 'test-instance-1',
          softwareVersion: 'event_sourcing_test@0.0.0',
        ),
        projections: ProjectionRegistry(),
        promoters: PromoterRegistry(),
      );
      addTearDown(store.close);

      expect(registry.isSealed, isTrue);
      expect(() => registry.register(_defn('after_open')), throwsArgumentError);
      expect(registry.byId('after_open'), isNull);
    });

    // EventStore.openForTest seals the registry it is handed as part of
    // its boot (the same seal EventStore.open performs). A registration
    // attempted after the open must be refused and leave the registry as
    // the boot read it.
    // Verifies: EVS-DEV-version-compatibility/K (registry complete at open)
    test(
      'EventStore.openForTest seals the entry-type registry it is given',
      () async {
        final db = await newDatabaseFactoryMemory().openDatabase(
          'entry-type-seal-${DateTime.now().microsecondsSinceEpoch}.db',
        );
        final backend = SembastBackend(database: db);
        final securityContexts = SembastSecurityContextStore(backend: backend);
        final registry = EntryTypeRegistry()..register(_defn('before_open'));

        await EventStore.openForTest(
          storage: backend,
          entryTypes: registry,
          source: const Source(
            hopId: 'test-server',
            identifier: 'test-instance-1',
            softwareVersion: 'event_sourcing_test@0.0.0',
          ),
          securityContexts: securityContexts,
        );

        expect(registry.isSealed, isTrue);
        expect(
          () => registry.register(_defn('after_open')),
          throwsArgumentError,
        );
        expect(registry.byId('after_open'), isNull);
        expect(registry.byId('before_open'), isNotNull);
      },
    );
  });
}
