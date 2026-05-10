import 'package:event_sourcing/src/promoters/primitives/transform.dart';

class PromoterSpec {
  final String viewName;
  final String entryType;
  final int fromVersion;
  final int toVersion;
  final List<TransformPrimitive> transforms;

  const PromoterSpec({
    required this.viewName,
    required this.entryType,
    required this.fromVersion,
    required this.toVersion,
    required this.transforms,
  });
}
