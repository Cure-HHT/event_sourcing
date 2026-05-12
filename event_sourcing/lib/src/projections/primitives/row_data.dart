// event_sourcing/lib/src/projections/primitives/row_data.dart
import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class RowDataExtractor {
  const RowDataExtractor();
  Map<String, Object?> extract(StoredEvent event);
}

class WholePayload extends RowDataExtractor {
  const WholePayload();
  @override
  Map<String, Object?> extract(StoredEvent event) =>
      Map<String, Object?>.unmodifiable(event.data);
}

class PayloadField extends RowDataExtractor {
  final String fieldName;
  const PayloadField(this.fieldName);

  @override
  Map<String, Object?> extract(StoredEvent event) {
    if (!event.data.containsKey(fieldName)) {
      return const <String, Object?>{};
    }
    final value = event.data[fieldName];
    if (value is! Map) {
      throw StateError(
        'PayloadField("$fieldName"): field on event ${event.eventId} '
        'is not a Map (got ${value.runtimeType})',
      );
    }
    return Map<String, Object?>.unmodifiable(Map<String, Object?>.from(value));
  }
}

class SelectedFields extends RowDataExtractor {
  final List<String> fieldNames;
  const SelectedFields(this.fieldNames);

  @override
  Map<String, Object?> extract(StoredEvent event) {
    final result = <String, Object?>{};
    for (final name in fieldNames) {
      if (event.data.containsKey(name)) {
        result[name] = event.data[name];
      }
    }
    return Map<String, Object?>.unmodifiable(result);
  }
}
