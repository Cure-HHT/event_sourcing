// Implements: EVS-PRD-canonical-json/A/B/C/D/E/F
//
// Barrel file: re-exports CanonicalJson, canonicalize, and canonicalizeBytes
// from src/canonical_json.dart, exposing the full RFC 8785 JCS surface as
// the package's public API.  All assertions are realized by the
// implementation in src/canonical_json.dart; this file carries the same
// annotation because it is the package's public entry point.

/// JSON Canonicalization Scheme (RFC 8785) for Dart.
///
/// See README.md for usage and NOTICE.md for attribution.
library;

export 'src/canonical_json.dart'
    show CanonicalJson, canonicalize, canonicalizeBytes;
