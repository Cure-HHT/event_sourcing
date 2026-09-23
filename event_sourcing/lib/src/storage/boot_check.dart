import 'package:event_sourcing/src/versions.dart';

/// The record an accepted `EventStore.open` writes in its boot transaction,
/// whether or not that boot had anything else to write.
///
/// Persisted in `backend_state` under the key `boot_check`, overwritten by
/// every accepted boot. It names the build that last opened the database:
/// its package version and data-format version, and when it opened.
class BootCheck {
  const BootCheck({
    required this.at,
    required this.packageVersion,
    required this.dataFormat,
  });

  /// Decode from the persisted JSON form. Throws [FormatException] on a
  /// missing or malformed field.
  factory BootCheck.fromJson(Map<String, Object?> json) {
    final at = json['at'];
    final packageVersion = json['package_version'];
    if (at is! String) {
      throw const FormatException('BootCheck: missing or non-string "at"');
    }
    if (packageVersion is! String) {
      throw const FormatException(
        'BootCheck: missing or non-string "package_version"',
      );
    }
    return BootCheck(
      at: DateTime.parse(at).toUtc(),
      packageVersion: packageVersion,
      dataFormat: DataFormatVersion.fromJson(json['data_format']),
    );
  }

  /// When the boot that wrote the record ran.
  final DateTime at;

  /// The package version of the build that wrote the record.
  final String packageVersion;

  /// The data-format version of the build that wrote the record.
  final DataFormatVersion dataFormat;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'at': at.toUtc().toIso8601String(),
    'package_version': packageVersion,
    'data_format': dataFormat.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      other is BootCheck &&
      other.at.isAtSameMomentAs(at) &&
      other.packageVersion == packageVersion &&
      other.dataFormat == dataFormat;

  @override
  int get hashCode =>
      Object.hash(at.microsecondsSinceEpoch, packageVersion, dataFormat);

  @override
  String toString() =>
      'BootCheck(at: ${at.toIso8601String()}, packageVersion: '
      '$packageVersion, dataFormat: $dataFormat)';
}
