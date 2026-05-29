// Verifies: EVS-PRD-library-charter/A/E
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('EventStoreConfig', () {
    test('creates production config with required fields', () {
      final config = EventStoreConfig.production(
        deviceId: 'test-device-123',
        userId: 'test-user-456',
        syncServerUrl: 'https://api.example.com',
        encryptionKey: 'test-key-789',
      );

      expect(config.deviceId, equals('test-device-123'));
      expect(config.userId, equals('test-user-456'));
      expect(config.syncServerUrl, equals('https://api.example.com'));
      expect(config.encryptionEnabled, isTrue);
      expect(config.encryptionKey, equals('test-key-789'));
    });

    test('creates development config with defaults', () {
      final config = EventStoreConfig.development(
        deviceId: 'dev-device-123',
        userId: 'dev-user-456',
      );

      expect(config.deviceId, equals('dev-device-123'));
      expect(config.userId, equals('dev-user-456'));
      expect(config.storageName, contains('dev'));
      expect(config.telemetryEnabled, isTrue);
    });

    test('enables encryption when encryption key provided', () {
      final config = EventStoreConfig.development(
        deviceId: 'test-device',
        encryptionKey: 'test-key',
      );

      expect(config.encryptionEnabled, isTrue);
      expect(config.encryptionKey, equals('test-key'));
    });

    test('copyWith creates new instance with updated values', () {
      final original = EventStoreConfig.development(deviceId: 'device-1');

      final updated = original.copyWith(
        deviceId: 'device-2',
        userId: 'user-123',
      );

      expect(updated.deviceId, equals('device-2'));
      expect(updated.userId, equals('user-123'));
      expect(original.deviceId, equals('device-1')); // Original unchanged
    });
  });

  group('EventStoreException', () {
    test('StorageBackendException contains message', () {
      const exception = StorageBackendException('Test error');

      expect(exception.message, equals('Test error'));
      expect(exception.toString(), contains('StorageBackendException'));
      expect(exception.toString(), contains('Test error'));
    });

    test('ChainVerificationException indicates security alert', () {
      const exception = ChainVerificationException(
        'Invalid signature',
        eventId: 'event-123',
      );

      expect(exception.message, equals('Invalid signature'));
      expect(exception.eventId, equals('event-123'));
      expect(exception.toString(), contains('🚨 SECURITY ALERT'));
    });
  });
}
