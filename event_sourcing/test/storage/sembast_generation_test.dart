// Verifies: EVS-DEV-version-compatibility/H
// a Sembast database outside the browser is used by one process: its
//   registration with the incompatible-generation guard holds nothing, and
//   two conflicting generations register on it in turn and at once.
@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/generation.dart'
    show UnguardedGenerationRegistration;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

GenerationDescriptor _descriptor(int major) => GenerationDescriptor(
  packageVersion: '0.5.0',
  dataFormat: const DataFormatVersion(2, 0),
  entryTypes: <String, EntryTypeVersion>{'x': EntryTypeVersion(major, 0)},
);

void main() {
  test('registerGeneration on io returns a registration that holds '
      'nothing', () async {
    final backend = SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase('gen.db'),
    );
    addTearDown(backend.close);
    final one = await backend.registerGeneration(_descriptor(1));
    final two = await backend.registerGeneration(_descriptor(2));
    expect(one, isA<UnguardedGenerationRegistration>());
    expect(two, isA<UnguardedGenerationRegistration>());
    expect(one.isLost, isFalse);
    await one.completeBoot();
    await two.completeBoot();
    await backend.transaction(one.recordInTxn);
    expect(await backend.transaction(backend.readDataGenerationTxn), isNull);
    await one.release();
    await two.release();
  });
}
