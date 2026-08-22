// Verifies: EVS-DEV-find-all-events-extended-filters/D
// the reference
//   SembastBackend SHALL realize the composed filter via a SINGLE shared
//   helper (`_composeFindAllEventsFilter`) used by BOTH the out-of-transaction
//   (`findAllEvents`) and in-transaction (`findAllEventsInTxn`) code paths.
//
// This assertion is structural — it constrains HOW the backend is built, not
// observable output — so it cannot be verified by the backend-agnostic
// conformance harness (which only sees behavior). The harness's behavioral
// A/B/C tests confirm both paths filter identically; this test confirms they
// do so through one shared helper rather than two divergent copies.
//
// Implemented as a source scan: the helper is private, so we assert against
// the library source that (a) the helper is declared exactly once, and (b)
// both public methods delegate to it. `findAllEvents` is declared before the
// helper and `findAllEventsInTxn` after it, so a call site existing on each
// side of the declaration proves both paths use it.
@TestOn('vm')
library;

import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

void main() {
  // `flutter test` runs with the package root as CWD.
  final source = File(
    'lib/src/storage/sembast_backend.dart',
  ).readAsStringSync();

  const decl = 'Filter? _composeFindAllEventsFilter(';
  const call = '_composeFindAllEventsFilter(';

  test('SembastBackend declares exactly one _composeFindAllEventsFilter '
      'helper', () {
    final declCount = decl.allMatches(source).length;
    expect(
      declCount,
      1,
      reason:
          'expected a single shared filter-compose helper, found '
          '$declCount declarations',
    );
  });

  test('both findAllEvents and findAllEventsInTxn delegate to the shared '
      'helper', () {
    final declIndex = source.indexOf(decl);
    expect(declIndex, greaterThan(-1), reason: 'helper declaration not found');

    // findAllEvents is defined before the helper; findAllEventsInTxn after it.
    final beforeDecl = source.substring(0, declIndex);
    final afterDecl = source.substring(declIndex + decl.length);

    expect(
      beforeDecl.contains(call),
      isTrue,
      reason:
          'findAllEvents (out-of-transaction path, defined before the '
          'helper) must call _composeFindAllEventsFilter',
    );
    expect(
      afterDecl.contains(call),
      isTrue,
      reason:
          'findAllEventsInTxn (in-transaction path, defined after the '
          'helper) must call _composeFindAllEventsFilter',
    );
  });
}
