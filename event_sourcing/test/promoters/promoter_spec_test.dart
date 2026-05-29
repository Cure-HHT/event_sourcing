// Verifies: EVS-PRD-materializer/A
// Verifies: EVS-DEV-ingest-promotes-before-fold/C
// Verifies: EVS-DEV-snapshot-promotion-on-open/B
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('PromoterSpec exposes view, entry, version range, transforms', () {
    const spec = PromoterSpec(
      viewName: 'diary_entries',
      entryType: 'epistaxis_event',
      fromVersion: 1,
      toVersion: 2,
      transforms: [
        RenameField(sourceField: 'old_name', targetField: 'new_name'),
        DefaultField(fieldName: 'language', defaultValue: 'en'),
      ],
    );
    expect(spec.viewName, 'diary_entries');
    expect(spec.entryType, 'epistaxis_event');
    expect(spec.fromVersion, 1);
    expect(spec.toVersion, 2);
    expect(spec.transforms, hasLength(2));
  });
}
