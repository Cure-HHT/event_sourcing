# EVS-prd-canonical-json: Canonical JSON Serialization

**Level**: prd | **Status**: Draft | **Refines**: EVS-prd-library-charter

## Purpose

The `canonical_json_jcs` package provides RFC 8785 (JSON Canonicalization Scheme, JCS) serialization for Dart. Any two systems serializing the same logical Dart content produce identical UTF-8 byte sequences. This canonical form is the anchor of the library's tamper-evidence story: every event hash, signature, and cross-system comparison rests on it.

The package is intentionally narrow — it is a pure-Dart utility, dependency-free, and usable from any Cure-HHT component that needs canonical-form serialization without pulling the rest of the event-sourcing stack.

## Assertions

A. The package SHALL serialize Dart values to JSON in conformance with RFC 8785 (JSON Canonicalization Scheme).

B. The package SHALL produce identical UTF-8 byte sequences for any two inputs representing identical logical content.

C. The package SHALL accept the JSON-compatible Dart value types: `null`, `bool`, `int`, `double`, `String`, `List`, and `Map<String, dynamic>` with `String` keys.

D. The package SHALL produce a diagnostic error for inputs containing types it cannot serialize.

E. The package SHALL run identically on every Dart-supported platform (mobile, server, web, desktop).

F. The package SHALL be pure Dart.

## Rationale

**Why JCS?** RFC 8785 is the IETF-standardized canonicalization scheme. Adopting it instead of inventing a custom canonical form lets independent verifiers reproduce hashes without learning Cure-HHT-specific conventions, and lets Cure-HHT components share canonical-form contracts with any third-party tooling that already speaks JCS.

**Why a separate package?** Canonical-form serialization is reusable beyond event sourcing. Any component that signs, hashes, or audits structured payloads can depend on `canonical_json_jcs` directly without inheriting the event-log machinery.

**Why pure Dart?** The package must run identically on every tier — mobile clients producing canonical hashes, server tiers verifying them. Any platform-specific branching would break byte-identical output across tiers, defeating the package's purpose.

*End* *Canonical JSON Serialization* | **Hash**: 00000000
