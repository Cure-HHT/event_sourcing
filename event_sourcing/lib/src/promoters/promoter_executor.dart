// event_sourcing/lib/src/promoters/promoter_executor.dart
// Implements: EVS-DEV-ingest-promotes-before-fold/A/B
// Implements: EVS-DEV-snapshot-promotion-on-open/B/D
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';

class PromoterExecutor {
  static Map<String, Object?> promote({
    required PromoterRegistry registry,
    required String viewName,
    required String entryType,
    required int fromVersion,
    required int toVersion,
    required Map<String, Object?> payload,
  }) {
    final chain = registry.chain(
      viewName: viewName,
      entryType: entryType,
      fromVersion: fromVersion,
      toVersion: toVersion,
    );
    var current = Map<String, Object?>.unmodifiable(payload);
    for (final spec in chain) {
      current = TransformChain.applyAll(spec.transforms, current);
    }
    return current;
  }
}
