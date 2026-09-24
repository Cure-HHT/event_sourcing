// Resolved-element scan of the library's public surface.
//
// Builds the analyzer's resolved element model of `lib/` and exposes the
// rules the public-surface test enforces. Every rule is a pure function of
// resolved elements or resolved syntax trees and returns the list of
// violations it found, so the same rule runs against the real library and
// against the synthetic fixtures the negative tests analyze.
import 'dart:io';

import 'package:analyzer/dart/analysis/analysis_context_collection.dart';
import 'package:analyzer/dart/analysis/results.dart';
import 'package:analyzer/dart/ast/ast.dart';
import 'package:analyzer/dart/ast/visitor.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:analyzer/file_system/overlay_file_system.dart';
import 'package:analyzer/file_system/physical_file_system.dart';
import 'package:path/path.dart' as p;

/// Absolute path of the `event_sourcing` package root. `flutter test` runs
/// with the package root as the working directory.
String packageRoot() => Directory.current.path;

/// Path of the Dart SDK the analyzer resolves `dart:` libraries against.
/// Under `flutter test` the running executable is the Flutter tester, so
/// the SDK is located through `FLUTTER_ROOT`; under a plain Dart VM it is
/// the running `dart` executable's SDK.
String dartSdkPath() {
  final flutterRoot = Platform.environment['FLUTTER_ROOT'];
  if (flutterRoot != null && flutterRoot.isNotEmpty) {
    return p.join(flutterRoot, 'bin', 'cache', 'dart-sdk');
  }
  final exe = File(Platform.resolvedExecutable);
  if (p.basenameWithoutExtension(exe.path) == 'dart') {
    return exe.parent.parent.path;
  }
  throw StateError(
    'cannot locate the Dart SDK: FLUTTER_ROOT is unset and the running '
    'executable (${exe.path}) is not the dart VM',
  );
}

/// One analysis of `lib/`, optionally with in-memory fixture files laid
/// over the file system at paths relative to the package root.
class SurfaceScanner {
  SurfaceScanner({Map<String, String> overlays = const <String, String>{}})
    : root = packageRoot() {
    final provider = OverlayResourceProvider(PhysicalResourceProvider.INSTANCE);
    overlays.forEach((relative, content) {
      provider.setOverlay(
        p.join(root, relative),
        content: content,
        modificationStamp: 0,
      );
    });
    _collection = AnalysisContextCollection(
      includedPaths: <String>[p.join(root, 'lib')],
      resourceProvider: provider,
      sdkPath: dartSdkPath(),
    );
  }

  final String root;
  late final AnalysisContextCollection _collection;

  /// The resolved library at [relative] (a path under the package root),
  /// or null when the file is a part of another library.
  Future<LibraryElement?> library(String relative) async {
    final path = p.join(root, relative);
    final result = await _collection
        .contextFor(path)
        .currentSession
        .getResolvedLibrary(path);
    if (result is NotLibraryButPartResult) return null;
    if (result is! ResolvedLibraryResult) {
      throw StateError('could not resolve $relative: $result');
    }
    return result.element;
  }

  /// The resolved syntax trees of the library at [relative], or an empty
  /// list when the file is a part of another library.
  Future<List<CompilationUnit>> units(String relative) async {
    final path = p.join(root, relative);
    final result = await _collection
        .contextFor(path)
        .currentSession
        .getResolvedLibrary(path);
    if (result is NotLibraryButPartResult) return const <CompilationUnit>[];
    if (result is! ResolvedLibraryResult) {
      throw StateError('could not resolve $relative: $result');
    }
    return <CompilationUnit>[for (final u in result.units) u.unit];
  }

  /// Every library under the directory [relative] on disk.
  Future<List<LibraryElement>> librariesUnder(String relative) async {
    final dir = Directory(p.join(root, relative));
    final files =
        dir
            .listSync(recursive: true)
            .whereType<File>()
            .where((f) => f.path.endsWith('.dart'))
            .map((f) => p.relative(f.path, from: root))
            .toList()
          ..sort();
    final libraries = <LibraryElement>[];
    for (final file in files) {
      final lib = await library(file);
      if (lib != null) libraries.add(lib);
    }
    return libraries;
  }
}

/// Every class declared in [libraries].
Iterable<ClassElement> classesOf(Iterable<LibraryElement> libraries) =>
    libraries.expand((l) => l.classes);

/// The class named [name] in [libraries]; throws when absent.
ClassElement classNamed(Iterable<LibraryElement> libraries, String name) =>
    classesOf(libraries).firstWhere(
      (c) => c.name == name,
      orElse: () => throw StateError('class $name not found'),
    );

/// The public instance and static members an [InstanceElement] declares
/// itself, keyed by name (setters as `name=`). Fields are included under
/// their own name; the getters and setters a field induces are not listed
/// separately.
Map<String, Element> declaredMembers(InstanceElement owner) {
  final members = <String, Element>{};
  for (final field in owner.fields) {
    final name = field.name;
    if (field.isOriginGetterSetter || name == null || !field.isPublic) continue;
    members[name] = field;
  }
  for (final method in owner.methods) {
    final name = method.name;
    if (name == null || !method.isPublic) continue;
    members[name] = method;
  }
  for (final getter in owner.getters) {
    final name = getter.name;
    if (getter.isOriginVariable || name == null || !getter.isPublic) continue;
    members[name] = getter;
  }
  for (final setter in owner.setters) {
    final name = setter.name;
    if (setter.isOriginVariable || name == null || !setter.isPublic) continue;
    members['$name='] = setter;
  }
  return members;
}

bool isInternal(Element element) => element.metadata.hasInternal;

/// Whether [element], or the class or extension that declares it, carries
/// `@internal`.
bool isInternalOrOwnerInternal(Element element) {
  if (isInternal(element)) return true;
  final owner = element.enclosingElement;
  return owner is InstanceElement && isInternal(owner);
}

// ---------------------------------------------------------------------------
// Rule (a): StorageBackend members.
// ---------------------------------------------------------------------------

/// Name prefixes that mark a storage member as a read (`watch` streams
/// what a read returns as it changes).
const storageReadPrefixes = <String>[
  'read',
  'find',
  'list',
  'has',
  'query',
  'watch',
];

/// Whether [name] is a read by the naming rule of the reads category.
bool isReadName(String name) =>
    name == 'wedgedFifos' || storageReadPrefixes.any(name.startsWith);

/// Every member of [contract] that is on neither allowlist carries
/// `@internal` on its declaration and on every override in
/// [implementations]; the reads category admits only read-named members.
List<String> storageBackendRule({
  required InterfaceElement contract,
  required Iterable<InterfaceElement> implementations,
  required Map<String, String> reads,
  required Map<String, String> nonReads,
  bool checkStale = true,
}) {
  final violations = <String>[];
  for (final name in reads.keys) {
    if (!isReadName(name)) {
      violations.add(
        'the reads category holds "$name", whose name does not mark a read; '
        'move it to the named non-read category or mark it @internal',
      );
    }
  }
  final members = declaredMembers(contract);
  for (final name in <String>[...reads.keys, ...nonReads.keys]) {
    final declared = members[name];
    if (declared != null && isInternal(declared)) {
      violations.add(
        '${contract.name}.$name is on an allowlist yet carries @internal',
      );
    }
    for (final impl in implementations) {
      final override = declaredMembers(impl)[name];
      if (override != null && isInternal(override)) {
        violations.add(
          '${impl.name}.$name is on an allowlist yet carries @internal',
        );
      }
    }
  }
  if (checkStale) {
    for (final name in <String>[...reads.keys, ...nonReads.keys]) {
      if (!members.containsKey(name)) {
        violations.add(
          'the allowlist names "$name", which ${contract.name} does not '
          'declare',
        );
      }
    }
  }
  for (final entry in members.entries) {
    final name = entry.key;
    if (reads.containsKey(name) || nonReads.containsKey(name)) continue;
    if (!isInternal(entry.value)) {
      violations.add(
        '${contract.name}.$name is on neither allowlist and lacks @internal',
      );
    }
    for (final impl in implementations) {
      final override = declaredMembers(impl)[name];
      if (override != null && !isInternal(override)) {
        violations.add(
          '${impl.name}.$name overrides an internal member without '
          '@internal',
        );
      }
    }
  }
  return violations;
}

/// Every public member that a concrete backend (or an extension on one) in
/// [owners] declares beyond [contract] is read-named, on [allowlist] (with
/// its reason), or carries `@internal`.
List<String> concreteBackendRule({
  required InterfaceElement contract,
  required Iterable<InstanceElement> owners,
  required Map<String, String> allowlist,
  bool checkStale = true,
}) {
  final violations = <String>[];
  final contractMembers = declaredMembers(contract).keys.toSet();
  final seen = <String>{};
  for (final owner in owners) {
    for (final entry in declaredMembers(owner).entries) {
      final name = entry.key;
      if (contractMembers.contains(name)) continue;
      final key = '${owner.name}.$name';
      seen.add(key);
      if (isInternalOrOwnerInternal(entry.value)) continue;
      if (isReadName(name) || allowlist.containsKey(key)) continue;
      violations.add(
        '$key is a concrete-only member that is neither read-named, on the '
        'concrete allowlist, nor @internal',
      );
    }
  }
  if (checkStale) {
    for (final key in allowlist.keys) {
      if (!seen.contains(key)) {
        violations.add(
          'the concrete allowlist names $key, which no backend '
          'declares',
        );
      }
    }
  }
  return violations;
}

// ---------------------------------------------------------------------------
// Rule (b): exports the barrel must not carry.
// ---------------------------------------------------------------------------

List<String> forbiddenExportsRule(
  LibraryElement barrel,
  Set<String> forbidden,
) {
  final exported = barrel.exportNamespace.definedNames2.keys.toSet();
  return <String>[
    for (final name in forbidden)
      if (exported.contains(name)) 'the barrel exports $name',
  ];
}

// ---------------------------------------------------------------------------
// Rule (c): raw database handles and function-typed state on the backends.
// ---------------------------------------------------------------------------

/// Whether [type], one of its type arguments, one of its record fields, or
/// a parameter or return type of a function type in it, is, or is a subtype
/// of, a raw handle to the database or to a transaction's underlying engine
/// transaction (so a `Future<Database>`, a `Stream<Transaction>`, a
/// `List<Pool>` or a `(Session, int)` each count).
bool isRawHandleType(DartType type) {
  final t = type;
  if (t is RecordType) {
    return <DartType>[
      ...t.positionalFields.map((f) => f.type),
      ...t.namedFields.map((f) => f.type),
    ].any(isRawHandleType);
  }
  if (t is FunctionType) {
    return isRawHandleType(t.returnType) ||
        t.formalParameters.any((p) => isRawHandleType(p.type));
  }
  if (t is! InterfaceType) return false;
  if (t.typeArguments.any(isRawHandleType)) return true;
  final candidates = <InterfaceElement>[
    t.element,
    ...t.element.allSupertypes.map((s) => s.element),
  ];
  for (final c in candidates) {
    final uri = c.library.uri.toString();
    if (uri.startsWith('package:sembast/') &&
        (c.name == 'Database' || c.name == 'Transaction')) {
      return true;
    }
    if (uri.startsWith('package:postgres/') &&
        (c.name == 'Pool' || c.name == 'Session' || c.name == 'Connection')) {
      return true;
    }
  }
  return false;
}

DartType? _exposedType(Element member) => switch (member) {
  FieldElement(:final type) => type,
  GetterElement(:final returnType) => returnType,
  MethodElement(:final returnType) => returnType,
  _ => null,
};

/// No public member of [owners] exposes a raw handle (as a return type, a
/// field type or a setter's parameter type) unless it is one of
/// [sanctioned] (qualified `Owner.member`) and carries `@internal`; no
/// public field or setter of [functionTypedOwners] has a function type.
List<String> rawHandleRule({
  required Iterable<InstanceElement> owners,
  required Set<String> sanctioned,
  Iterable<InstanceElement> functionTypedOwners = const <InstanceElement>[],
}) {
  final violations = <String>[];
  for (final owner in functionTypedOwners) {
    for (final entry in declaredMembers(owner).entries) {
      final key = '${owner.name}.${entry.key}';
      final member = entry.value;
      if (member is FieldElement && member.type is FunctionType) {
        violations.add('$key is a public field of function type');
      }
      if (member is SetterElement &&
          member.formalParameters.any((p) => p.type is FunctionType)) {
        violations.add('$key is a public setter of function type');
      }
    }
  }
  for (final owner in owners) {
    for (final entry in declaredMembers(owner).entries) {
      final key = '${owner.name}.${entry.key}';
      final member = entry.value;
      final exposed = member is SetterElement
          ? member.formalParameters.firstOrNull?.type
          : _exposedType(member);
      if (exposed == null || !isRawHandleType(exposed)) continue;
      if (!sanctioned.contains(key)) {
        violations.add('$key returns a raw database or transaction handle');
      } else if (!isInternal(member)) {
        violations.add('$key returns a raw handle and lacks @internal');
      }
    }
  }
  return violations;
}

/// Every class and extension declared in [libraries].
List<InstanceElement> instanceOwners(Iterable<LibraryElement> libraries) =>
    <InstanceElement>[
      for (final lib in libraries) ...<InstanceElement>[
        ...lib.classes,
        ...lib.extensions,
        ...lib.mixins,
      ],
    ];

/// The classes and extensions in [libraries] that are, or extend, the
/// classes named [backendNames].
List<InstanceElement> backendOwners(
  Iterable<LibraryElement> libraries,
  Set<String> backendNames,
) {
  final owners = <InstanceElement>[];
  for (final lib in libraries) {
    for (final c in lib.classes) {
      final names = <String?>{
        c.name,
        ...c.allSupertypes.map((s) => s.element.name),
      };
      if (names.any(backendNames.contains)) owners.add(c);
    }
    for (final e in lib.extensions) {
      final extended = e.extendedType;
      if (extended is InterfaceType &&
          backendNames.contains(extended.element.name)) {
        owners.add(e);
      }
    }
  }
  return owners;
}

// ---------------------------------------------------------------------------
// Rule (d): no test seams on the exported surface.
// ---------------------------------------------------------------------------

/// Substrings that mark a name as a test seam, a test switch or a log sink.
const seamNameMarkers = <String>['hook', 'seam', 'fortest', 'sink'];

/// One parameter, field, getter, setter or return type on the exported
/// surface.
typedef SurfaceEntry = ({String name, DartType type, bool isReturn});

/// Every parameter, field, getter and setter on the exported surface of
/// [barrel], and every exported function's and method's return type,
/// keyed `Owner.member(param)`, `Owner.member (field)`,
/// `Owner.member (getter)`, `Owner.member=(param)`, `name (variable)` or
/// `Owner.member (return)`.
Map<String, SurfaceEntry> exportedSurface(LibraryElement barrel) {
  final found = <String, SurfaceEntry>{};
  void params(String owner, List<FormalParameterElement> ps) {
    for (final param in ps) {
      final name = param.name;
      if (name != null) {
        found['$owner($name)'] = (
          name: name,
          type: param.type,
          isReturn: false,
        );
      }
    }
  }

  for (final element in barrel.exportNamespace.definedNames2.values) {
    if (element is TopLevelFunctionElement) {
      params('${element.name}', element.formalParameters);
      found['${element.name} (return)'] = (
        name: '${element.name}',
        type: element.returnType,
        isReturn: true,
      );
    }
    if (element is TopLevelVariableElement) {
      found['${element.name} (variable)'] = (
        name: '${element.name}',
        type: element.type,
        isReturn: false,
      );
    }
    if (element is TypeAliasElement) {
      found['${element.name} (typedef)'] = (
        name: '${element.name}',
        type: element.aliasedType,
        isReturn: true,
      );
    }
    if (element is! InstanceElement) continue;
    final owner = element.name;
    if (element is InterfaceElement) {
      for (final c in element.constructors) {
        if (c.isPublic) params('$owner.${c.name}', c.formalParameters);
      }
    }
    for (final entry in declaredMembers(element).entries) {
      final member = entry.value;
      switch (member) {
        case FieldElement(:final type):
          found['$owner.${entry.key} (field)'] = (
            name: entry.key,
            type: type,
            isReturn: false,
          );
        case GetterElement(:final returnType):
          found['$owner.${entry.key} (getter)'] = (
            name: entry.key,
            type: returnType,
            isReturn: false,
          );
        case SetterElement():
          params('$owner.${entry.key}', member.formalParameters);
        case MethodElement():
          params('$owner.${entry.key}', member.formalParameters);
          found['$owner.${entry.key} (return)'] = (
            name: entry.key,
            type: member.returnType,
            isReturn: true,
          );
        default:
          break;
      }
    }
  }
  return found;
}

/// Whether [type], or any type it is built from (type arguments, record
/// fields, function parameters and return), is declared in a library under
/// `lib/src/testing/`.
bool mentionsTestingType(DartType type) {
  final alias = type.alias;
  if (alias != null &&
      (alias.element.library.uri.toString().contains('/src/testing/') ||
          alias.typeArguments.any(mentionsTestingType))) {
    return true;
  }
  if (type is RecordType) {
    return <DartType>[
      ...type.positionalFields.map((f) => f.type),
      ...type.namedFields.map((f) => f.type),
    ].any(mentionsTestingType);
  }
  if (type is FunctionType) {
    return mentionsTestingType(type.returnType) ||
        type.formalParameters.any((p) => mentionsTestingType(p.type));
  }
  if (type is InterfaceType) {
    return type.element.library.uri.toString().contains('/src/testing/') ||
        type.typeArguments.any(mentionsTestingType);
  }
  return false;
}

/// No exported parameter, field, getter or setter, of any type, carries a
/// seam-marked name; every function-typed one is on [allowlist] (keys as
/// in [exportedSurface]); no exported signature mentions a type declared
/// under `lib/src/testing/`; nothing under `lib/src/testing/` is exported.
List<String> seamSurfaceRule({
  required LibraryElement barrel,
  required Map<String, String> allowlist,
  bool checkStale = true,
}) {
  final violations = <String>[];
  final surface = exportedSurface(barrel);
  final functionTyped = <String>{};
  surface.forEach((key, entry) {
    if (mentionsTestingType(entry.type)) {
      violations.add('$key mentions a type declared under lib/src/testing/');
    }
    if (entry.isReturn) return;
    final lower = entry.name.toLowerCase();
    if (seamNameMarkers.any(lower.contains)) {
      violations.add('$key carries a seam name');
    }
    if (entry.type is FunctionType) {
      functionTyped.add(key);
      if (!allowlist.containsKey(key)) {
        violations.add('$key is a function-typed surface not on the allowlist');
      }
    }
  });
  if (checkStale) {
    for (final key in allowlist.keys) {
      if (!functionTyped.contains(key)) {
        violations.add(
          'the function-typed allowlist names $key, which the '
          'barrel does not export',
        );
      }
    }
  }
  for (final element in barrel.exportNamespace.definedNames2.values) {
    final uri = element.library?.uri.toString() ?? '';
    if (uri.contains('/src/testing/')) {
      violations.add('the barrel exports ${element.name} from $uri');
    }
  }
  return violations;
}

/// Collects the zone reads and direct log calls in one syntax tree.
class _ZoneAndLogVisitor extends RecursiveAstVisitor<void> {
  final List<String> zoneReads = <String>[];
  final List<String> directLogs = <String>[];

  @override
  void visitIndexExpression(IndexExpression node) {
    final target = node.realTarget.staticType;
    if (target is InterfaceType &&
        target.element.name == 'Zone' &&
        target.element.library.uri.toString() == 'dart:async') {
      zoneReads.add(node.toSource());
    }
    super.visitIndexExpression(node);
  }

  @override
  void visitMethodInvocation(MethodInvocation node) {
    final element = node.methodName.element;
    if (element is TopLevelFunctionElement) {
      final uri = element.library.uri.toString();
      if ((uri == 'dart:core' && element.name == 'print') ||
          (uri == 'dart:developer' && element.name == 'log')) {
        directLogs.add(node.toSource());
      }
    }
    super.visitMethodInvocation(node);
  }
}

/// No library in [units] (path to resolved syntax trees) other than the
/// seam file indexes a `Zone`, however the zone is reached, except the
/// exact reads [allowedReads] names for a path (each a production guard
/// that can only refuse a call, never a seam).
List<String> zoneReadRule(
  Map<String, List<CompilationUnit>> units, {
  Map<String, Set<String>> allowedReads = const <String, Set<String>>{},
}) {
  final violations = <String>[];
  units.forEach((path, trees) {
    if (path.endsWith('src/testing/delivery_test_hooks.dart')) return;
    final allowed = allowedReads[path] ?? const <String>{};
    for (final tree in trees) {
      final visitor = _ZoneAndLogVisitor();
      tree.accept(visitor);
      for (final read in visitor.zoneReads) {
        if (allowed.contains(read)) continue;
        violations.add('$path reads a zone value: $read');
      }
    }
  });
  return violations;
}

/// No library in [units] other than the internal logger calls `print` or
/// `dart:developer` `log` directly.
List<String> directLoggingRule(Map<String, List<CompilationUnit>> units) {
  final violations = <String>[];
  units.forEach((path, trees) {
    if (path.endsWith('src/logging.dart')) return;
    for (final tree in trees) {
      final visitor = _ZoneAndLogVisitor();
      tree.accept(visitor);
      for (final call in visitor.directLogs) {
        violations.add('$path logs directly: $call');
      }
    }
  });
  return violations;
}

// ---------------------------------------------------------------------------
// Rule (f): members that must be internal, and the library operations.
// ---------------------------------------------------------------------------

/// Each `Owner.member` in [required] exists in [owners], and each plain
/// `name` in [required] exists among [functions]; each carries `@internal`.
List<String> mustBeInternalRule({
  required Iterable<InstanceElement> owners,
  required Map<String, String> required,
  Iterable<TopLevelFunctionElement> functions =
      const <TopLevelFunctionElement>[],
}) {
  final violations = <String>[];
  for (final key in required.keys) {
    final dot = key.indexOf('.');
    if (dot < 0) {
      final matches = functions.where((f) => f.name == key);
      if (matches.isEmpty) {
        violations.add('$key: function not found');
      } else if (!isInternal(matches.first)) {
        violations.add('$key lacks @internal');
      }
      continue;
    }
    final ownerName = key.substring(0, dot);
    final memberName = key.substring(dot + 1);
    final matches = owners.where((o) => o.name == ownerName);
    if (matches.isEmpty) {
      violations.add('$key: $ownerName not found');
      continue;
    }
    final member = declaredMembers(matches.first)[memberName];
    if (member == null) {
      violations.add('$key: member not found');
    } else if (!isInternal(member)) {
      violations.add('$key lacks @internal');
    }
  }
  return violations;
}

/// Every exported top-level function of [barrel] is on [topLevelOperations]
/// or carries `@internal`; every public member of [bundle] is on
/// [bundleOperations] or carries `@internal`.
List<String> libraryOperationsRule({
  required LibraryElement barrel,
  required Map<String, String> topLevelOperations,
  required InstanceElement bundle,
  required Map<String, String> bundleOperations,
}) {
  final violations = <String>[];
  for (final element in barrel.exportNamespace.definedNames2.values) {
    if (element is! TopLevelFunctionElement) continue;
    final name = element.name!;
    if (!topLevelOperations.containsKey(name) && !isInternal(element)) {
      violations.add(
        'exported top-level function $name is neither a named library '
        'operation nor @internal',
      );
    }
  }
  for (final entry in declaredMembers(bundle).entries) {
    if (!bundleOperations.containsKey(entry.key) && !isInternal(entry.value)) {
      violations.add(
        '${bundle.name}.${entry.key} is neither a named library operation '
        'nor @internal',
      );
    }
  }
  return violations;
}

// ---------------------------------------------------------------------------
// Rule (g): the unexported surface a `src/` import reaches.
// ---------------------------------------------------------------------------

/// Every public top-level function in [libraries] that [barrel] does not
/// export, and every public member of a public class, mixin or extension
/// that [barrel] does not export, carries `@internal` (on itself or on its
/// owner) or is on [operations] (keys `name` or `Owner.member`) with a
/// reason. Constructors are not checked: creating an object changes no
/// persisted state.
List<String> unexportedSurfaceRule({
  required Iterable<LibraryElement> libraries,
  required LibraryElement barrel,
  required Map<String, String> operations,
  bool checkStale = true,
}) {
  final violations = <String>[];
  final exported = barrel.exportNamespace.definedNames2.values.toSet();
  final seen = <String>{};
  for (final lib in libraries) {
    for (final f in lib.topLevelFunctions) {
      final name = f.name;
      if (name == null || !f.isPublic || exported.contains(f)) continue;
      seen.add(name);
      if (isInternal(f) || operations.containsKey(name)) continue;
      violations.add(
        'unexported top-level function $name is neither a named library '
        'operation nor @internal',
      );
    }
    for (final owner in <InstanceElement>[
      ...lib.classes,
      ...lib.mixins,
      ...lib.extensions,
      ...lib.enums,
    ]) {
      final ownerName = owner.name;
      if (ownerName == null || ownerName.startsWith('_')) continue;
      if (exported.contains(owner) || isInternal(owner)) continue;
      for (final entry in declaredMembers(owner).entries) {
        final key = '$ownerName.${entry.key}';
        seen.add(key);
        if (isInternal(entry.value) || operations.containsKey(key)) continue;
        violations.add(
          '$key (unexported) is neither a named library operation nor '
          '@internal',
        );
      }
    }
  }
  if (checkStale) {
    for (final key in operations.keys) {
      if (!seen.contains(key)) {
        violations.add(
          'the unexported-operations list names $key, which no unexported '
          'library declares',
        );
      }
    }
  }
  return violations;
}
