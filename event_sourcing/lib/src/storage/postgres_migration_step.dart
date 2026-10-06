// Implements: EVS-DEV-postgres-backend/G
// a migration step names the schema version it leads to, the minimum
//   compatible schema version it records, and its DDL.
import 'package:meta/meta.dart' show internal;

/// One step of the Postgres schema's migration list: the DDL that brings a
/// schema at the previous step's version to [toVersion], and the minimum
/// schema version a build must support to open a database at [toVersion]
/// ([minCompatibleVersion]).
///
/// A step that only adds (a table, a nullable column, an index) keeps the
/// minimum, so a build that provisioned the previous version still opens
/// the database; a step that an older build cannot run against raises it.
@internal
final class PostgresMigrationStep {
  /// A step to [toVersion] recording [minCompatibleVersion], applying [ddl]
  /// in order.
  const PostgresMigrationStep({
    required this.toVersion,
    required this.minCompatibleVersion,
    required this.ddl,
  });

  /// The schema version the database is at once this step has run.
  final int toVersion;

  /// The lowest schema version a build may support and still open a
  /// database at [toVersion].
  final int minCompatibleVersion;

  /// The statements of the step, each run on its own, in order.
  final List<String> ddl;
}
