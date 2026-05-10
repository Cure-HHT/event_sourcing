import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';

sealed class ProjectionSpec {
  const ProjectionSpec();
  String get viewName;
  SubscriptionFilter get interest;
}

class AggregateProjectionSpec extends ProjectionSpec {
  @override
  final String viewName;
  @override
  final SubscriptionFilter interest;
  final String aggregateType;
  final Set<String> tombstoneEventTypes;
  final List<DerivedField> derivedFields;

  const AggregateProjectionSpec({
    required this.viewName,
    required this.aggregateType,
    required this.interest,
    required this.tombstoneEventTypes,
    this.derivedFields = const [],
  });
}

class TableProjectionSpec extends ProjectionSpec {
  @override
  final String viewName;
  @override
  final SubscriptionFilter interest;
  final Set<String> insertEventTypes;
  final Set<String> removeEventTypes;
  final RowKeyExtractor rowKey;
  final RowDataExtractor rowData;

  const TableProjectionSpec({
    required this.viewName,
    required this.interest,
    required this.insertEventTypes,
    required this.removeEventTypes,
    required this.rowKey,
    required this.rowData,
  });
}
