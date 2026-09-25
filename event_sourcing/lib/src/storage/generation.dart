// Implements: EVS-DEV-version-compatibility/F
// GenerationDescriptor.conflictsWith is the live comparison of two data
//   generations: another data-format major, or another major of an entry
//   type both register.
// Implements: EVS-DEV-version-compatibility/I
// GenerationRecord.admits is the one durable comparison, used by the boot
//   and by the Postgres transaction fence alike.
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

/// The data generation of one build of the library: what a live instance
/// registers while its event store is open, and what the database records
/// when its boot commits.
///
/// A generation is made of components, each of which a live instance holds
/// on its own: the library's data-format major ([dataFormat]) and the major
/// of each entry type the build registers ([entryTypes]). Two generations
/// conflict ([conflictsWith]) when their data-format majors differ, or when
/// an entry type both register has different majors. An entry type only
/// one of them registers never conflicts: adding an entry type is a
/// compatible change. Minors never conflict.
final class GenerationDescriptor {
  /// The generation of a build of package version [packageVersion], data
  /// format [dataFormat], registering [entryTypes].
  GenerationDescriptor({
    required this.packageVersion,
    required this.dataFormat,
    required Map<String, EntryTypeVersion> entryTypes,
  }) : entryTypes = Map<String, EntryTypeVersion>.unmodifiable(entryTypes);

  /// The package version of the build. Recorded for diagnostics; it
  /// decides nothing.
  final String packageVersion;

  /// The data-format version of the build.
  final DataFormatVersion dataFormat;

  /// The registered version of every entry type the build registers, by
  /// entry-type id.
  final Map<String, EntryTypeVersion> entryTypes;

  /// The generation's components, one per data-format major and per
  /// registered entry type: `data_format:<major>` and
  /// `entry_type:<id>:<major>`, sorted.
  List<String> get components {
    final ids = entryTypes.keys.toList()..sort();
    return <String>[
      generationComponent(kind: 'data_format', id: '', value: dataFormat.major),
      for (final id in ids)
        generationComponent(
          kind: 'entry_type',
          id: id,
          value: entryTypes[id]!.major,
        ),
    ];
  }

  /// True when this generation and [other] cannot share a database: their
  /// data-format majors differ, or an entry type both register has
  /// different majors.
  bool conflictsWith(GenerationDescriptor other) =>
      conflictingComponents(other).isNotEmpty;

  /// The components of [other] that conflict with this generation, in the
  /// form [components] writes them.
  List<String> conflictingComponents(GenerationDescriptor other) {
    final out = <String>[];
    if (other.dataFormat.major != dataFormat.major) {
      out.add(
        generationComponent(
          kind: 'data_format',
          id: '',
          value: other.dataFormat.major,
        ),
      );
    }
    final ids = other.entryTypes.keys.toList()..sort();
    for (final id in ids) {
      final mine = entryTypes[id];
      final theirs = other.entryTypes[id]!;
      if (mine != null && mine.major != theirs.major) {
        out.add(
          generationComponent(kind: 'entry_type', id: id, value: theirs.major),
        );
      }
    }
    return out;
  }

  @override
  String toString() =>
      'GenerationDescriptor(package $packageVersion, ${components.join(', ')})';
}

/// Writes one generation component: `data_format:<major>` for the data
/// format (whose [id] is empty), `<kind>:<id>:<value>` otherwise.
@internal
String generationComponent({
  required String kind,
  required String id,
  required int value,
}) => id.isEmpty ? '$kind:$value' : '$kind:$id:$value';

/// The highest data generation that has booted on a database, recorded in
/// the database by every boot that commits.
///
/// [admits] is the one comparison the library makes against it: at boot,
/// and, on a backend where a running instance can lose its registration, at
/// the start of every transaction.
final class GenerationRecord {
  /// A record of data-format major [dataFormatMajor] and the highest major
  /// any committed boot registered for each entry type in
  /// [entryTypeMajors].
  GenerationRecord({
    required this.dataFormatMajor,
    required Map<String, int> entryTypeMajors,
  }) : entryTypeMajors = Map<String, int>.unmodifiable(entryTypeMajors);

  /// Parses the stored shape written by [toJson].
  ///
  /// Throws [FormatException] naming what is missing or malformed.
  factory GenerationRecord.fromJson(Object? json) {
    if (json is! Map) {
      throw FormatException('GenerationRecord: expected a map, got $json');
    }
    final major = json['data_format_major'];
    final majors = json['entry_type_majors'];
    if (major is! int || major < 1) {
      throw FormatException(
        'GenerationRecord: data_format_major is not a positive integer: '
        '$major',
      );
    }
    if (majors is! Map) {
      throw FormatException(
        'GenerationRecord: entry_type_majors is not a map: $majors',
      );
    }
    final parsed = <String, int>{};
    for (final entry in majors.entries) {
      final key = entry.key;
      final value = entry.value;
      if (key is! String || value is! int || value < 1) {
        throw FormatException(
          'GenerationRecord: entry_type_majors[$key] is not a positive '
          'integer: $value',
        );
      }
      parsed[key] = value;
    }
    return GenerationRecord(dataFormatMajor: major, entryTypeMajors: parsed);
  }

  /// The record of a first boot of [descriptor].
  factory GenerationRecord.of(GenerationDescriptor descriptor) =>
      GenerationRecord(
        dataFormatMajor: descriptor.dataFormat.major,
        entryTypeMajors: <String, int>{
          for (final entry in descriptor.entryTypes.entries)
            entry.key: entry.value.major,
        },
      );

  /// The data-format major of every build that has booted on the database.
  final int dataFormatMajor;

  /// The highest major any committed boot registered, by entry-type id.
  final Map<String, int> entryTypeMajors;

  /// False when the record holds another data-format major than
  /// [descriptor]'s, or, for an entry type [descriptor] registers, a
  /// higher major; true otherwise. A recorded major below the
  /// descriptor's is a stop-then-start major bump, which the boot of that
  /// descriptor raises.
  bool admits(GenerationDescriptor descriptor) =>
      refusedComponent(descriptor) == null;

  /// The first reason this record does not admit [descriptor], or null
  /// when it admits it: `data_format:<recorded major>` or
  /// `entry_type:<id>:<recorded major>`.
  String? refusedComponent(GenerationDescriptor descriptor) {
    if (dataFormatMajor != descriptor.dataFormat.major) {
      return generationComponent(
        kind: 'data_format',
        id: '',
        value: dataFormatMajor,
      );
    }
    final ids = descriptor.entryTypes.keys.toList()..sort();
    for (final id in ids) {
      final recorded = entryTypeMajors[id];
      if (recorded != null && recorded > descriptor.entryTypes[id]!.major) {
        return generationComponent(kind: 'entry_type', id: id, value: recorded);
      }
    }
    return null;
  }

  /// The record after a boot of [descriptor] commits: [descriptor]'s
  /// data-format major, and for each entry type the higher of the recorded
  /// major and the one [descriptor] registers. [descriptor] must be
  /// admitted.
  GenerationRecord merge(GenerationDescriptor descriptor) {
    final majors = Map<String, int>.of(entryTypeMajors);
    descriptor.entryTypes.forEach((id, version) {
      final recorded = majors[id];
      if (recorded == null || recorded < version.major) {
        majors[id] = version.major;
      }
    });
    return GenerationRecord(
      dataFormatMajor: descriptor.dataFormat.major,
      entryTypeMajors: majors,
    );
  }

  /// `{'data_format_major': M, 'entry_type_majors': {id: M, ...}}`.
  Map<String, Object?> toJson() => <String, Object?>{
    'data_format_major': dataFormatMajor,
    'entry_type_majors': Map<String, int>.of(entryTypeMajors),
  };

  @override
  bool operator ==(Object other) {
    if (other is! GenerationRecord) return false;
    if (other.dataFormatMajor != dataFormatMajor) return false;
    if (other.entryTypeMajors.length != entryTypeMajors.length) return false;
    for (final entry in entryTypeMajors.entries) {
      if (other.entryTypeMajors[entry.key] != entry.value) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(
    dataFormatMajor,
    Object.hashAllUnordered(
      entryTypeMajors.entries.map((e) => Object.hash(e.key, e.value)),
    ),
  );

  @override
  String toString() =>
      'GenerationRecord(data_format:$dataFormatMajor, $entryTypeMajors)';
}

/// The state of a backend's generation registrations.
enum GenerationStatus {
  /// Every open event store's generation is registered with the backend's
  /// guard.
  registered,

  /// The backend lost the session its registrations were held on and has
  /// not registered them again yet. Its transactions are still checked
  /// against the database's generation record.
  lost,

  /// A generation the backend serves is no longer admitted: a conflicting
  /// build booted, or an incompatible schema was provisioned, while it was
  /// not registered. Every transaction throws [GenerationFencedException];
  /// the instance must be stopped.
  fenced,
}

/// One live registration of a data generation with a backend's
/// incompatible-generation guard, taken by `EventStore.open` before its
/// boot transaction.
///
/// A backend shared by several processes or tabs implements the guard; a
/// backend used by one process returns a registration that holds nothing.
abstract class GenerationRegistration {
  const GenerationRegistration();

  /// True when the backend lost what this registration held (a lock session
  /// that ended) and has not taken it again.
  bool get isLost;

  /// Called inside the boot transaction: persists whatever the backend
  /// needs to inspect this registration later (on Postgres, the records
  /// that map lock keys back to components; elsewhere nothing).
  @internal
  Future<void> recordInTxn(Transaction txn);

  /// Called once the boot transaction committed: the registration joins
  /// the backend's active set and the exclusive boot lock is released.
  Future<void> completeBoot();

  /// Gives up everything this registration took (its component locks, and
  /// the boot lock while still held) and removes it from the backend's
  /// active set. Idempotent.
  Future<void> release();
}

/// A registration that holds nothing: the guard of a Sembast database
/// outside the browser, which is used by one isolate of one process.
@internal
final class UnguardedGenerationRegistration extends GenerationRegistration {
  /// Creates a registration that holds nothing.
  const UnguardedGenerationRegistration();

  @override
  bool get isLost => false;

  @override
  @internal
  Future<void> recordInTxn(Transaction txn) async {}

  @override
  Future<void> completeBoot() async {}

  @override
  Future<void> release() async {}
}

/// Thrown by `EventStore.open`, before any write, when another live
/// instance of the library on the same database holds a data generation
/// that conflicts with the opening build's; and by
/// `PostgresBackend.provision`, writing nothing, when a live instance
/// requires a schema version below the minimum compatible version the
/// provisioning would record.
class IncompatibleGenerationException implements Exception {
  /// A refusal naming [conflictingComponents].
  const IncompatibleGenerationException({
    required this.conflictingComponents,
    required this.descriptor,
  });

  /// The components held by a live instance that conflict, in the form
  /// `data_format:<major>`, `entry_type:<id>:<major>` or `schema:<n>`.
  final List<String> conflictingComponents;

  /// The generation of the refused open, or null for a refused
  /// provisioning.
  final GenerationDescriptor? descriptor;

  @override
  String toString() {
    final what = descriptor == null
        ? 'the provisioning'
        : 'this build (${descriptor!.components.join(', ')})';
    return 'IncompatibleGenerationException: a live instance of the library '
        'on this database holds ${conflictingComponents.join(', ')}, which '
        'conflicts with $what. Builds of another data-format major, or of '
        'another major of an entry type they both register, do not run '
        'side by side: stop every instance of the other build first '
        '(a stop-then-start deployment). Nothing was written.';
  }
}

/// Thrown by every transaction of a backend whose data generation is no
/// longer admitted by the database: a conflicting build booted, or an
/// incompatible schema was provisioned, while this instance was not
/// registered. Nothing the transaction wrote is committed. The instance
/// must be stopped.
class GenerationFencedException implements Exception {
  /// A refusal giving [reason].
  const GenerationFencedException(this.reason);

  /// Why the transaction was refused.
  final String reason;

  @override
  String toString() =>
      'GenerationFencedException: $reason. This instance can no longer '
      'commit to the database and must be stopped.';
}

/// Thrown when the incompatible-generation guard cannot run as configured:
/// the browser page has no lock manager (Web Locks exist only in a secure
/// context), or a boot or a provisioning of the same database held the
/// boot lock for longer than the wait allowed.
class GenerationGuardConfigurationException implements Exception {
  /// A refusal giving [message].
  const GenerationGuardConfigurationException(this.message);

  /// What is wrong and what the deployment must provide.
  final String message;

  @override
  String toString() => 'GenerationGuardConfigurationException: $message';
}
