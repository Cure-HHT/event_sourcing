# Naming conventions

Naming rules for public functions, types, members, and parameters across
the `event_sourcing` repo and its sibling packages (`reaction`,
`reaction_widgets`, `reaction_widgets_testing`, `canonical_json_jcs`,
`provenance`). The library is a domain-neutral substrate; names read as if
authored fresh for this library, not carried over from any origin system.

- **R1 — Effective Dart casing.** Types/enums/extensions/typedefs
  `UpperCamelCase`; members/functions/getters/variables/parameters/
  constants `lowerCamelCase`; files/libraries `lowercase_with_underscores`
  matching the primary public symbol. Acronyms cased as words (`Uuid`,
  `httpClient`, `Jcs`), consistently.
- **R2 — No heritage/provenance names.** Describe the symbol's role in
  *this* library — never the system it was extracted from, nor a type that
  no longer exists here. No carryover names from any system this library
  was extracted from, and no consumer-domain leakage.
- **R3 — No temporal/versioning qualifiers.** No `legacy`, `old`, `new`,
  `current` (as a disambiguator), `v2`, `2`, `Ex`. History lives in git.
- **R4 — Domain-neutral substrate vocabulary.** event, aggregate,
  projection, view, action, dispatch, scope, permission, role,
  destination, ingest, source, hop, promoter, entry type, subscription.
  No domain types baked into names.
- **R5 — Don't restate the type, kind, or library.** No library prefix;
  avoid vague kind-suffixes (`Data`, `Object`, `Info`, `Manager`,
  `Helper`, `Util`, `Wrapper`). Meaningful role suffixes are fine
  (`Registry`, `Store`, `Spec`, `Policy`, `Result`, `Verdict`, `Update`).
- **R6 — Functions are verbs; values are nouns/adjectives.** Imperative
  verb phrase for behavior (`append`, `subscribe`, `open`); noun/adjective
  for getters/fields, no `get` prefix. One verb per concept.
- **R7 — Factory / entry-point naming.** Prefer a named constructor or
  static factory on the owning type over a free function; where a
  top-level composition helper is warranted, name it for the verb + what
  it returns in substrate terms. (Established `bootstrap*` family verb is
  acceptable; the heritage *noun* is not.)
- **R8 — Booleans read as predicates.** `is`/`has`/`can`/`allows`/
  `requires`/`should`. No bare nouns or verb prefixes (`enable…`) for
  boolean fields.
- **R9 — Abbreviations: only well-established, used consistently.**
  Allowed: `id`, `json`, `http`, `ws`, `uuid`, `fifo`, `jcs`, `db`,
  `spec`. Expand the rest (`txn`→`transaction`, `defn`→`definition`,
  `ctx`→`context`, `cfg`→`config`).
- **R10 — Parameters name the role, not the type or position.** No `str`,
  `val`, `obj`, `arg`, single letters on public APIs.
- **R11 — Consistency across parallel concepts.** Sibling symbols share
  structure; rename whole sets, not one-offs that break symmetry.
- **R12 — Names must not mislead.** The name matches behavior (a
  non-throwing checker is not `assertX`; an I/O getter is `fetch`/`load`;
  a field holding a `ConnectionStatus` is not `error`).
- **R13 — Public surface gets the strictest scrutiny.** Exported symbols
  are the interface contract; private (`_x`) names follow R1/R6 at lower
  priority.
