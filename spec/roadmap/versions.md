# Roadmap — versions and the data generation

Deferred work on the library's versions: the package version, the
data-format version and the entry-type versions, and the data generation
the incompatible-generation guard compares
(`spec/dev-version-compatibility.md`).

## Projection specifications as part of the data generation

**Baseline.** The data generation the guard compares is the data-format
major and each registered entry type's major. A change to a projection
specification is not versioned: two builds with different specifications
for one view do not conflict, and a view is caught up with the log only
for the entry types its interest names and for a whole-view pair (`spec/roadmap/projections.md`).

**Remaining.** Version each projection specification (a declared version,
or a digest of its definition) as a component of the data generation, so
that builds that fold one view differently are refused side by side, the
same way builds of different entry-type majors are, and a changed
definition makes the view converge again after the next open.

## Reading an older data-format major

**Baseline.** A build opens only a database of its own data-format major:
another major is refused before anything is written
(`DataFormatIncompatibleError`), and a database an earlier data format
wrote is refused by name (`DatabaseResetRequiredError`). A major step is
therefore deployed on an empty database, or after a restore.

**Remaining.** A release that reads a database of the previous data-format
major without altering its events. The log is append-only and the hash of
the event at each sequence is a Layer 1 fact, so a migration leaves every
stored event as it is. Two shapes fit: a versioned reader that decodes
events of the older major at read and fold time, so the new build folds the
existing log; or a migration, run as a separate provisioning-style step
under the boot lock and refused while any instance of the old major is
live, into a new log whose first entry links to the old log's final hash,
with the old log kept intact and readable. Rewriting stored events in
place and re-chaining their hashes is not an option: it would contradict
the append-only log and hash-chain integrity, and would need a
charter-level amendment.
