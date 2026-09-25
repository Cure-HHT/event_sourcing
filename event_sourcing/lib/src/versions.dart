// Implements: EVS-DEV-version-compatibility/A
// EntryTypeVersion identifies an entry-type version by a major and a minor
//   number; isCompatibleWith is true exactly when the majors are equal.
// Implements: EVS-DEV-version-compatibility/C
// DataFormatVersion is the library's data-format version, a major and a
//   minor number distinct from the package version; builds with the same
//   data-format major are compatible.

/// Version of one registered entry type: a major and a minor number.
///
/// Two versions of an entry type are compatible exactly when their majors
/// are equal. The library enforces what it can check: a promoter step
/// within one major is `DefaultField` only (a minor step may also have no
/// promoter), a rename or a drop needs a step to minor 0 of the next major,
/// and open and ingest compare majors. That builds registering different
/// minors of one major read and write each other's events also rests on a
/// precondition the consumer keeps: a minor bump only adds optional
/// fields, and producers do not rename, drop or re-type a field within a
/// major.
///
/// Serialised as `{'major': M, 'minor': m}` in event records, view target
/// versions and the batch wire format, and written `M.m` by [toString],
/// the form the registry audit and the snapshot-promotion audit record.
final class EntryTypeVersion implements Comparable<EntryTypeVersion> {
  /// Creates the version `major.minor`. [major] is at least 1 and [minor]
  /// at least 0.
  const EntryTypeVersion(this.major, this.minor)
    : assert(major >= 1, 'major must be >= 1'),
      assert(minor >= 0, 'minor must be >= 0');

  /// Parses `{'major': M, 'minor': m}`.
  ///
  /// Throws [FormatException] naming the key when [json] is not a map, a
  /// component is missing or not an integer, the major is below 1, or the
  /// minor is below 0. Any other key is ignored.
  factory EntryTypeVersion.fromJson(Object? json) {
    final (major, minor) = _parseComponents(json, 'EntryTypeVersion');
    return EntryTypeVersion(major, minor);
  }

  /// The major number. A different major is an incompatible version.
  final int major;

  /// The minor number within [major].
  final int minor;

  /// True when [other] has the same major.
  bool isCompatibleWith(EntryTypeVersion other) => major == other.major;

  /// The next minor version of the same major.
  EntryTypeVersion get nextMinor => EntryTypeVersion(major, minor + 1);

  /// Orders by major, then minor.
  @override
  int compareTo(EntryTypeVersion other) => major != other.major
      ? major.compareTo(other.major)
      : minor.compareTo(other.minor);

  bool operator <(EntryTypeVersion other) => compareTo(other) < 0;
  bool operator <=(EntryTypeVersion other) => compareTo(other) <= 0;
  bool operator >(EntryTypeVersion other) => compareTo(other) > 0;
  bool operator >=(EntryTypeVersion other) => compareTo(other) >= 0;

  /// `{'major': M, 'minor': m}`.
  Map<String, Object?> toJson() => <String, Object?>{
    'major': major,
    'minor': minor,
  };

  @override
  bool operator ==(Object other) =>
      other is EntryTypeVersion && other.major == major && other.minor == minor;

  @override
  int get hashCode => Object.hash(EntryTypeVersion, major, minor);

  /// `M.m`.
  @override
  String toString() => '$major.$minor';
}

/// Version of the library's data format: a major and a minor number,
/// distinct from the package version.
///
/// The data format is everything a build of the library stores or sends:
/// the stored shape of events and of the records kept beside them, the
/// reserved entry types and their payloads, and the batch wire format.
/// Builds whose data-format majors are equal are compatible: they read and
/// write one database, and they ingest each other's events.
///
/// The maintainers' rule for a change of the data format: a minor bump may
/// only add what an older build of the same major ignores or reads
/// correctly -- a new reserved entry type, a new optional field in a
/// reserved event or in a persisted record whose absence keeps the old
/// behaviour, a new persisted record, or additive schema changes. Anything
/// else is a major bump. The aggregate type and event types the library
/// declares for each reserved entry type are fixed within a major: a build
/// refuses at ingest a reserved event outside the shapes it declares, so
/// adding an event type to a reserved entry type, or changing its aggregate
/// type, is a major bump, and a new kind of reserved event is a new
/// reserved entry type.
///
/// Serialised as `{'major': M, 'minor': m}` in event records and the batch
/// wire format, and written `M.m` by [toString].
final class DataFormatVersion implements Comparable<DataFormatVersion> {
  /// Creates the version `major.minor`. [major] is at least 1 and [minor]
  /// at least 0.
  const DataFormatVersion(this.major, this.minor)
    : assert(major >= 1, 'major must be >= 1'),
      assert(minor >= 0, 'minor must be >= 0');

  /// Parses `{'major': M, 'minor': m}`.
  ///
  /// Throws [FormatException] naming the key when [json] is not a map, a
  /// component is missing or not an integer, the major is below 1, or the
  /// minor is below 0. Any other key is ignored.
  factory DataFormatVersion.fromJson(Object? json) {
    final (major, minor) = _parseComponents(json, 'DataFormatVersion');
    return DataFormatVersion(major, minor);
  }

  /// The major number. A different major is an incompatible data format.
  final int major;

  /// The minor number within [major].
  final int minor;

  /// True when [other] has the same major.
  bool isCompatibleWith(DataFormatVersion other) => major == other.major;

  /// The next minor version of the same major.
  DataFormatVersion get nextMinor => DataFormatVersion(major, minor + 1);

  /// Orders by major, then minor.
  @override
  int compareTo(DataFormatVersion other) => major != other.major
      ? major.compareTo(other.major)
      : minor.compareTo(other.minor);

  bool operator <(DataFormatVersion other) => compareTo(other) < 0;
  bool operator <=(DataFormatVersion other) => compareTo(other) <= 0;
  bool operator >(DataFormatVersion other) => compareTo(other) > 0;
  bool operator >=(DataFormatVersion other) => compareTo(other) >= 0;

  /// `{'major': M, 'minor': m}`.
  Map<String, Object?> toJson() => <String, Object?>{
    'major': major,
    'minor': minor,
  };

  @override
  bool operator ==(Object other) =>
      other is DataFormatVersion &&
      other.major == major &&
      other.minor == minor;

  @override
  int get hashCode => Object.hash(DataFormatVersion, major, minor);

  /// `M.m`.
  @override
  String toString() => '$major.$minor';
}

(int, int) _parseComponents(Object? json, String typeName) {
  if (json is! Map) {
    throw FormatException(
      '$typeName: expected a map with "major" and "minor", got '
      '${json.runtimeType}',
    );
  }
  final major = json['major'];
  if (major is! int) {
    throw FormatException('$typeName: missing or non-integer "major"');
  }
  if (major < 1) {
    throw FormatException('$typeName: "major" must be >= 1, got $major');
  }
  final minor = json['minor'];
  if (minor is! int) {
    throw FormatException('$typeName: missing or non-integer "minor"');
  }
  if (minor < 0) {
    throw FormatException('$typeName: "minor" must be >= 0, got $minor');
  }
  // Another key is one a later release of this data-format major may add;
  // it is not part of the version. A record that carries the map keeps it
  // whole (`StoredEvent.fromMap`), so the hash over it is unchanged.
  return (major, minor);
}
