// event_sourcing/lib/src/projections/primitives/row_data.dart
//
// Implements: EVS-PRD-materializer/A — RowDataExtractor (WholePayload,
//   PayloadField, SelectedFields, CombinedRowData, ConstantFields,
//   ListLengthField, EventTimestampField) are library-supplied primitives that the
//   TableProjectionSpec fold uses to extract row data from an event; they
//   are part of the library's materializer rule set.
// Implements: EVS-PRD-materializer/B
// each extractor is a pure function of
//   the StoredEvent; same input always yields same output.
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
  const PayloadField(this.fieldName);
  final String fieldName;

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
  const SelectedFields(this.fieldNames);
  final List<String> fieldNames;

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

/// The fields every one of [parts] extracts, merged in order: a field that
/// a later part also extracts takes the later part's value.
class CombinedRowData extends RowDataExtractor {
  const CombinedRowData(this.parts);
  final List<RowDataExtractor> parts;

  @override
  Map<String, Object?> extract(StoredEvent event) =>
      Map<String, Object?>.unmodifiable(<String, Object?>{
        for (final part in parts) ...part.extract(event),
      });
}

/// The same fields, with the same values, for every event.
class ConstantFields extends RowDataExtractor {
  const ConstantFields(this.values);
  final Map<String, Object?> values;

  @override
  Map<String, Object?> extract(StoredEvent event) =>
      Map<String, Object?>.unmodifiable(values);
}

/// One field, [fieldName], holding the length of the list in the event's
/// data field [sourceField]; nothing when the event carries no
/// [sourceField] or carries null there.
class ListLengthField extends RowDataExtractor {
  const ListLengthField(this.fieldName, this.sourceField);
  final String fieldName;
  final String sourceField;

  @override
  Map<String, Object?> extract(StoredEvent event) {
    final value = event.data[sourceField];
    if (value == null) return const <String, Object?>{};
    if (value is! List) {
      throw StateError(
        'ListLengthField("$sourceField"): field on event ${event.eventId} '
        'is not a List (got ${value.runtimeType})',
      );
    }
    return Map<String, Object?>.unmodifiable(<String, Object?>{
      fieldName: value.length,
    });
  }
}

/// One field, [fieldName], holding the event's client timestamp in UTC, as
/// an ISO-8601 string.
class EventTimestampField extends RowDataExtractor {
  const EventTimestampField(this.fieldName);
  final String fieldName;

  @override
  Map<String, Object?> extract(StoredEvent event) =>
      Map<String, Object?>.unmodifiable(<String, Object?>{
        fieldName: event.clientTimestamp.toUtc().toIso8601String(),
      });
}
