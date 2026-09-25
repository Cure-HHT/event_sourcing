// The public surface of the types whose instances the library hands to
// application code, as the analyzer resolves it, and the rules the
// handed-out-surface test enforces over it. Each rule is a pure function
// of resolved elements, so it runs against the real library and against
// the fixtures the negative tests lay over it. This file declares no
// tests, so it carries no citation.
import 'dart:io';

import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:path/path.dart' as p;

import '../test_support/surface_scan.dart';

/// The committed surface list, relative to the package root.
const String committedSurfacePath = 'test/barrier/handed_out_surface.txt';

/// Every type whose instances the library hands to application code, or
/// whose instances are reachable from one through its members: the public
/// types the application names, and the private run-time types of the
/// objects typed as a public interface (a private class's public members
/// are reachable through a dynamic call).
const List<String> handedOutTypes = <String>[
  // The event store and what it hands out.
  'EventStore',
  'EntryTypeRegistry',
  'ProjectionRegistry',
  'PromoterRegistry',
  'StorageReader',
  '_StorageReader',
  'SecurityContextStore',
  '_SecurityContextReader',
  'IdempotencyStore',
  'PostgresIdempotencyStore',
  // The bootstrap bundle, the destination registry and the delivery cycle.
  'EventStoreBundle',
  'DestinationRegistry',
  'SyncCycle',
  // What a transaction body receives.
  'PublishCollector',
  'Transaction',
  '_SembastTxn',
  '_PostgresTxn',
  // The policy and the dispatcher the library builds over the reader.
  'TableBackedAuthorizationPolicy',
  'ActionDispatcher',
  // The values and results the operations return.
  'StoredEvent',
  'IngestBatchResult',
  'PerEventIngestOutcome',
  'RetentionResult',
  'ChainVerdict',
  'DeliveryStatus',
  'DestinationSchedule',
  'TombstoneAndRefillResult',
  // Boot progress and subscription emissions.
  'BootProgress',
  'Update',
  'Delta',
  'Snapshot',
  'Tombstone',
  'EndOfReplay',
];

/// The class, mixin or extension type named [name] among [libraries];
/// null when none is declared.
InterfaceElement? interfaceNamed(
  Iterable<LibraryElement> libraries,
  String name,
) {
  final found = <InterfaceElement>[
    for (final library in libraries)
      for (final c in <InterfaceElement>[
        ...library.classes,
        ...library.mixins,
        ...library.extensionTypes,
      ])
        if (c.name == name) c,
  ];
  if (found.length > 1) {
    throw StateError(
      '$name is declared in ${found.length} libraries: '
      '${found.map((c) => c.library.uri).join(', ')}',
    );
  }
  return found.isEmpty ? null : found.single;
}

String _render(Element e) => e.displayString();

/// The members of [type] accessible outside the Dart library that
/// declares each of them, as `Type.member: signature` lines: its public
/// constructors (on a public type), its public static members, and the
/// public instance members it declares or inherits from any supertype
/// other than `Object`.
List<String> surfaceOf(InterfaceElement type) {
  final owner = type.name!;
  final lines = <String, String>{};
  void put(String member, String signature) =>
      lines.putIfAbsent(member, () => '$owner.$member: $signature');

  if (type.isPublic) {
    for (final c in type.constructors) {
      if (!c.isPublic) continue;
      final name = c.name;
      put(
        name == null || name == 'new' ? 'new' : name,
        'constructor ${_render(c)}',
      );
    }
  }
  final chain = <InterfaceElement>[
    type,
    for (final s in type.allSupertypes)
      if (!(s.element.name == 'Object' &&
          s.element.library.uri.toString() == 'dart:core'))
        s.element,
  ];
  for (final element in chain) {
    final inherited = !identical(element, type);
    for (final field in element.fields) {
      final name = field.name;
      if (field.isOriginGetterSetter || name == null || !field.isPublic) {
        continue;
      }
      if (inherited && field.isStatic) continue;
      final settable = field.setter != null && !field.isFinal && !field.isConst;
      put(
        name,
        '${field.isStatic ? 'static ' : ''}field ${_render(field)}'
        '${settable ? ' (settable)' : ''}',
      );
    }
    for (final getter in element.getters) {
      final name = getter.name;
      if (getter.isOriginVariable || name == null || !getter.isPublic) {
        continue;
      }
      if (inherited && getter.isStatic) continue;
      put(name, '${getter.isStatic ? 'static ' : ''}${_render(getter)}');
    }
    for (final setter in element.setters) {
      final name = setter.name;
      if (setter.isOriginVariable || name == null || !setter.isPublic) {
        continue;
      }
      if (inherited && setter.isStatic) continue;
      put('$name=', '${setter.isStatic ? 'static ' : ''}${_render(setter)}');
    }
    for (final method in element.methods) {
      final name = method.name;
      if (name == null || !method.isPublic) continue;
      if (inherited && method.isStatic) continue;
      put(name, '${method.isStatic ? 'static ' : ''}${_render(method)}');
    }
  }
  return lines.values.toList()..sort();
}

/// The surface of every type in [types] among [libraries], one line per
/// member, sorted; a type that is not declared contributes a line saying
/// so, which no committed list holds.
List<String> handedOutSurface(
  Iterable<LibraryElement> libraries, {
  List<String> types = handedOutTypes,
}) {
  final lines = <String>[];
  for (final name in types) {
    final type = interfaceNamed(libraries, name);
    if (type == null) {
      lines.add('$name: NOT DECLARED');
      continue;
    }
    lines.addAll(surfaceOf(type));
  }
  return lines..sort();
}

/// The committed list's lines: every non-empty line that is not a `#`
/// comment.
List<String> readCommittedSurface(String root) =>
    File(p.join(root, committedSurfacePath))
        .readAsLinesSync()
        .where((l) => l.trim().isNotEmpty && !l.startsWith('#'))
        .toList();

/// The differences between [actual] and [committed]: each member the
/// surface has that the list lacks, and each the list has that the surface
/// lacks.
List<String> surfaceDifferences({
  required List<String> actual,
  required List<String> committed,
}) {
  final a = actual.toSet();
  final c = committed.toSet();
  return <String>[
    for (final line in a.difference(c)) 'not in the committed list: $line',
    for (final line in c.difference(a)) 'not on the surface: $line',
  ]..sort();
}

// ---------------------------------------------------------------------------
// Transaction handles yield no engine handle (DEV /F).
// ---------------------------------------------------------------------------

/// Every class among [libraries] that is, or extends, the library's
/// `Transaction` handle type.
List<ClassElement> transactionHandleClasses(
  Iterable<LibraryElement> libraries,
) => <ClassElement>[
  for (final c in classesOf(libraries))
    if (c.name == 'Transaction' ||
        c.allSupertypes.any(
          (s) =>
              s.element.name == 'Transaction' &&
              s.element.library.uri.toString().endsWith(
                'src/storage/transaction.dart',
              ),
        ))
      c,
];

/// No member of a transaction handle class accessible outside its
/// declaring library -- a field, a getter, a method's return type, or a
/// parameter of a callback a method takes -- is a raw handle to the
/// database or to the engine transaction.
List<String> handleYieldsNoEngineRule(Iterable<ClassElement> handles) {
  final violations = <String>[];
  for (final handle in handles) {
    for (final line in _publicMembersWithTypes(handle)) {
      if (isRawHandleType(line.type)) {
        violations.add(
          '${handle.name}.${line.name} yields a raw engine handle '
          '(${line.type.getDisplayString()})',
        );
      }
    }
  }
  return violations;
}

Iterable<({String name, DartType type})> _publicMembersWithTypes(
  InterfaceElement owner,
) sync* {
  for (final field in owner.fields) {
    final name = field.name;
    if (field.isOriginGetterSetter || name == null || !field.isPublic) {
      continue;
    }
    yield (name: name, type: field.type);
  }
  for (final getter in owner.getters) {
    final name = getter.name;
    if (getter.isOriginVariable || name == null || !getter.isPublic) continue;
    yield (name: name, type: getter.returnType);
  }
  for (final method in owner.methods) {
    final name = method.name;
    if (name == null || !method.isPublic) continue;
    yield (name: name, type: method.returnType);
    for (final parameter in method.formalParameters) {
      final type = parameter.type;
      if (type is FunctionType) {
        for (final inner in type.formalParameters) {
          yield (name: '$name(${parameter.name})', type: inner.type);
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// The library builds the stores over the storage it opened (DEV /I).
// ---------------------------------------------------------------------------

/// Every public constructor of a public class declared in a file under
/// one of [directories] (paths relative to the package root, parts
/// included) takes no storage backend, pool or raw database handle, unless
/// it is on [allowlist] (`Class.constructor`, `new` for the unnamed one)
/// with its reason; an allowlist entry that names no such constructor is
/// reported as stale.
List<String> noPublicConstructorOverStorageRule({
  required Iterable<LibraryElement> libraries,
  required String root,
  required List<String> directories,
  required Map<String, String> allowlist,
}) {
  final violations = <String>[];
  final seen = <String>{};
  bool under(Element e) {
    final path = e.firstFragment.libraryFragment?.source.fullName;
    if (path == null) return false;
    final relative = p.relative(path, from: root);
    return directories.any((d) => p.isWithin(d, relative));
  }

  for (final c in classesOf(libraries)) {
    if (!c.isPublic || !under(c)) continue;
    for (final ctor in c.constructors) {
      if (!ctor.isPublic) continue;
      final ctorName = ctor.name == null || ctor.name == 'new'
          ? 'new'
          : ctor.name!;
      final key = '${c.name}.$ctorName';
      final overStorage = ctor.formalParameters.where(
        (param) => isBackendOrPoolType(param.type),
      );
      if (overStorage.isEmpty) continue;
      if (allowlist.containsKey(key)) {
        seen.add(key);
        continue;
      }
      violations.add(
        '$key is a public constructor over storage '
        '(${overStorage.map((param) => '${param.type.getDisplayString()} ${param.name}').join(', ')})',
      );
    }
  }
  for (final key in allowlist.keys) {
    if (!seen.contains(key)) {
      violations.add('stale allowlist entry: $key takes no storage');
    }
  }
  return violations;
}
