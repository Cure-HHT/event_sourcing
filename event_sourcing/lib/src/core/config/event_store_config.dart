// Implements: EVS-PRD-library-charter/H/D
/// Configuration for the event store.
///
/// This class holds all configuration needed to initialize the event store,
/// including database paths, encryption settings, and sync endpoints.
class EventStoreConfig {
  const EventStoreConfig({
    required this.deviceId,
    this.databasePath,
    this.storageName = 'event_sourcing.db',
    this.encryptionEnabled = true,
    this.encryptionKey,
    this.userId,
    this.syncServerUrl,
    this.telemetryEnabled = false,
    this.telemetryEndpoint,
  }) : assert(
         !encryptionEnabled || encryptionKey != null,
         'encryptionKey must be provided when encryption is enabled',
       );

  /// Create a development configuration with sensible defaults.
  factory EventStoreConfig.development({
    required String deviceId,
    String? userId,
    String? encryptionKey,
  }) {
    return EventStoreConfig(
      deviceId: deviceId,
      userId: userId,
      storageName: 'event_sourcing_dev.db',
      encryptionEnabled: encryptionKey != null,
      encryptionKey: encryptionKey,
      telemetryEnabled: true,
    );
  }

  /// Create a production configuration.
  factory EventStoreConfig.production({
    required String deviceId,
    required String userId,
    required String syncServerUrl,
    String? encryptionKey,
  }) {
    return EventStoreConfig(
      deviceId: deviceId,
      userId: userId,
      syncServerUrl: syncServerUrl,
      encryptionEnabled: encryptionKey != null,
      encryptionKey: encryptionKey,
      telemetryEnabled: true,
    );
  }

  /// Path to the SQLite database file.
  /// If null, uses default application documents directory.
  final String? databasePath;

  /// Name of the database file.
  final String storageName;

  /// Enable SQLCipher encryption.
  final bool encryptionEnabled;

  /// Encryption key for SQLCipher.
  /// Only used if [encryptionEnabled] is true.
  final String? encryptionKey;

  /// User ID for audit trail.
  /// Must be set before appending events.
  final String? userId;

  /// Device ID for conflict resolution.
  /// Automatically generated if not provided.
  final String deviceId;

  /// Base URL for sync server API.
  /// Example: 'https://api.example.com/v1'
  final String? syncServerUrl;

  /// Enable OpenTelemetry tracing.
  final bool telemetryEnabled;

  /// OpenTelemetry endpoint.
  /// Only used if [telemetryEnabled] is true.
  final String? telemetryEndpoint;

  /// Copy with new values.
  EventStoreConfig copyWith({
    String? databasePath,
    String? storageName,
    bool? encryptionEnabled,
    String? encryptionKey,
    String? userId,
    String? deviceId,
    String? syncServerUrl,
    bool? telemetryEnabled,
    String? telemetryEndpoint,
  }) {
    return EventStoreConfig(
      databasePath: databasePath ?? this.databasePath,
      storageName: storageName ?? this.storageName,
      encryptionEnabled: encryptionEnabled ?? this.encryptionEnabled,
      encryptionKey: encryptionKey ?? this.encryptionKey,
      userId: userId ?? this.userId,
      deviceId: deviceId ?? this.deviceId,
      syncServerUrl: syncServerUrl ?? this.syncServerUrl,
      telemetryEnabled: telemetryEnabled ?? this.telemetryEnabled,
      telemetryEndpoint: telemetryEndpoint ?? this.telemetryEndpoint,
    );
  }
}
