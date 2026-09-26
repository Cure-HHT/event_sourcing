// The outstanding-finding mark every row of a default view carries, for
// tests that compare whole rows. This file declares no tests, so it carries
// no citation.

/// The reserved key under which a default view's row carries its marks.
const String kIntegrityKey = r'$integrity';

/// The `$integrity` entry of a row no security finding marks, to spread
/// into an expected row.
const Map<String, Object?> kUnmarked = <String, Object?>{
  kIntegrityKey: <String, Object?>{'security_findings': <String>[]},
};

/// The `$integrity` entry of a row the findings [findingIds] mark, in
/// ascending order, to spread into an expected row.
Map<String, Object?> markedBy(List<String> findingIds) => <String, Object?>{
  kIntegrityKey: <String, Object?>{
    'security_findings': List<String>.of(findingIds)..sort(),
  },
};
