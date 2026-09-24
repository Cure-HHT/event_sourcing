// bin/server.dart
// IMPLEMENTS REQUIREMENTS:
//   EVS-DEV-postgres-backend/D — exercises the PostgresBackend +
//     PostgresIdempotencyStore end-to-end when run with
//     `--backend=postgres`. Sembast remains the default. `--provision`
//     provisions the Postgres schema and exits; serving never runs DDL.
//
// The server listens before it opens the event store: `/livez` answers 200
// once it listens, and `/health` answers 503 with the boot's progress until
// the bootstrap returns, then 200. It starts a delivery cycle; several
// servers may share one Postgres database, and one of them drains it while
// the others stand by. The environment variable DEMO_CONFIGURATION_VERSION
// carries the deployment's revision identifier into the cycle's declared
// configuration.

import 'dart:async';
import 'dart:io';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:action_permissions_demo/server/demo_server_host.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:args/args.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:path/path.dart' as p;
import 'package:sembast/sembast_io.dart';
import 'package:sembast/sembast_memory.dart';

Future<void> main(List<String> args) async {
  final parser = ArgParser()
    ..addOption('port', defaultsTo: '8080', help: 'TCP port to bind on')
    ..addOption(
      'backend',
      allowed: <String>['sembast', 'postgres'],
      defaultsTo: 'sembast',
      help: 'Storage backend.',
    )
    ..addOption(
      'postgres-url',
      help:
          'Postgres URL (required when --backend=postgres). '
          'Example: postgres://evs:evs@localhost:5432/evs_demo',
    )
    ..addOption(
      'postgres-lock-url',
      help:
          'Postgres URL of the lock connection (optional; defaults to '
          '--postgres-url). It must be one server session: a direct '
          'connection or a session-mode proxy, never a transaction-mode '
          'pooler.',
    )
    ..addFlag(
      'provision',
      defaultsTo: false,
      help:
          'Provision the Postgres schema (create or migrate the tables) '
          'and exit without serving. Run once per deployment, before the '
          'servers start. Requires --backend=postgres.',
    )
    ..addOption(
      'postgres-ssl-mode',
      allowed: <String>['disable', 'require', 'verifyFull'],
      defaultsTo: 'require',
      help:
          'SSL mode for the Postgres connection (default: require). '
          'Use "disable" for local docker-compose without SSL; '
          '"verifyFull" for full certificate validation against a '
          'managed Postgres over the public internet.',
    )
    ..addFlag(
      'ephemeral',
      defaultsTo: false,
      help:
          'Run with an in-memory database (state lost on shutdown). '
          'Sembast-only; ignored for --backend=postgres.',
    )
    ..addOption(
      'data-dir',
      help:
          'Directory for the persistent sembast DB; defaults to '
          '\$XDG_DATA_HOME/action_permissions_demo (or ~/.local/share/...). '
          'Ignored for --backend=postgres.',
    )
    ..addOption(
      'permissions-yaml',
      defaultsTo: 'tool/permissions.yaml',
      help: 'Path to the permissions seed YAML',
    )
    ..addOption(
      'users-yaml',
      defaultsTo: 'tool/users.yaml',
      help: 'Path to the user-directory seed YAML',
    )
    ..addOption(
      'install-id',
      help:
          'Stable per-install identifier (UUIDv4). When omitted, a fresh '
          'one is generated on each boot — appropriate for ephemeral runs '
          'only.',
    );

  final ArgResults parsed;
  try {
    parsed = parser.parse(args);
  } on FormatException catch (e) {
    stderr.writeln('error: ${e.message}\n\n${parser.usage}');
    exitCode = 64; // EX_USAGE
    return;
  }

  final port = int.parse(parsed['port'] as String);
  final backendKind = parsed['backend'] as String;
  final postgresUrl = parsed['postgres-url'] as String?;
  final postgresSslMode = switch (parsed['postgres-ssl-mode'] as String) {
    'disable' => SslMode.disable,
    'require' => SslMode.require,
    'verifyFull' => SslMode.verifyFull,
    _ => throw StateError(
      'unreachable — ArgParser allowed list constrains this option',
    ),
  };
  final ephemeral = parsed['ephemeral'] as bool;
  final permissionsYamlPath = parsed['permissions-yaml'] as String;
  final usersYamlPath = parsed['users-yaml'] as String;

  if (backendKind == 'postgres' &&
      (postgresUrl == null || postgresUrl.isEmpty)) {
    stderr.writeln('error: --postgres-url is required when --backend=postgres');
    exitCode = 64; // EX_USAGE
    return;
  }
  final postgresLockUrl = parsed['postgres-lock-url'] as String?;
  if (parsed['provision'] as bool) {
    if (backendKind != 'postgres') {
      stderr.writeln('error: --provision requires --backend=postgres');
      exitCode = 64; // EX_USAGE
      return;
    }
    try {
      await PostgresBackend.provision(
        postgresUrl!,
        lockUrl: postgresLockUrl,
        sslMode: postgresSslMode,
      );
    } on Object catch (e) {
      if (!_isRefusal(e)) rethrow;
      stderr.writeln('error: provisioning refused: $e');
      exitCode = 1;
      return;
    }
    stdout.writeln(
      'provisioned the Postgres schema at version $postgresSchemaVersion',
    );
    return;
  }

  // For sembast we need a data directory for the optional persistent
  // file; for postgres the file system layout is irrelevant but we still
  // honour --install-id (and read/write a sidecar file when one is not
  // provided) so identity persists across boots.
  final dataDir = (backendKind == 'sembast' && !ephemeral)
      ? _resolveDataDir(parsed['data-dir'] as String?)
      : Directory.systemTemp.createTempSync('action_permissions_demo_');
  await Directory(dataDir.path).create(recursive: true);

  final permissionsYaml = await File(permissionsYamlPath).readAsString();
  final usersYaml = await File(usersYamlPath).readAsString();

  // For non-ephemeral sembast runs the install identifier should persist
  // across boots so events from the same install share an originator
  // identity. For postgres / ephemeral runs we treat the data dir as a
  // scratch directory and generate per boot when no override is given.
  final installId =
      (parsed['install-id'] as String?) ??
      await _resolveInstallId(
        dataDir,
        ephemeral: ephemeral || backendKind == 'postgres',
      );

  // Listen first, so a platform's startup and liveness probes (/livez) and
  // its readiness probe (/health) are answered while the event store boots.
  final host = await DemoServerHost.listen(port: port);
  stdout.writeln(
    'demo server listening on http://${host.http.address.host}:'
    '${host.http.port} (booting; /livez, /health)',
  );
  Future<void> failStartup(Object error) async {
    host.fail(error);
    await host.close();
    exitCode = 1;
  }

  final StorageBackend backend;
  final IdempotencyStore idempotencyStore;
  final String backendDescription;

  if (backendKind == 'postgres') {
    final PostgresBackend pg;
    try {
      pg = await PostgresBackend.open(
        url: postgresUrl!,
        lockUrl: postgresLockUrl,
        sslMode: postgresSslMode,
      );
    } on PostgresSchemaIncompatibleException catch (e) {
      stderr.writeln(
        'error: $e\n'
        'Provision the database first: dart run bin/server.dart '
        '--backend=postgres --postgres-url=<url> --provision',
      );
      await failStartup(e);
      return;
    } on LockSessionConfigurationException catch (e) {
      stderr.writeln('error: $e');
      await failStartup(e);
      return;
    }
    backend = pg;
    idempotencyStore = PostgresIdempotencyStore.forBackend(pg);
    backendDescription = 'postgres ($postgresUrl, ssl=${postgresSslMode.name})';
  } else {
    final dbPath = p.join(dataDir.path, 'demo.db');
    final Database db = ephemeral
        ? await databaseFactoryMemory.openDatabase('demo')
        : await databaseFactoryIo.openDatabase(dbPath);
    backend = SembastBackend(database: db);
    idempotencyStore = DemoIdempotencyStore();
    backendDescription = 'sembast ($dbPath, ephemeral=$ephemeral)';
  }

  final DemoServerComponents components;
  try {
    components = await bootstrapDemoServer(
      backend: backend,
      idempotencyStore: idempotencyStore,
      permissionsYaml: permissionsYaml,
      usersYaml: usersYaml,
      installIdentifier: installId,
      deliveryLog: stdout.writeln,
      onBootProgress: host.health.record,
    );
  } on Object catch (e) {
    // The library refused to open the database: a server of another major
    // is running against it (a major bump is deployed stop-then-start), the
    // database records a newer generation, or it must be reset.
    if (!_isRefusal(e)) {
      await host.close();
      rethrow;
    }
    stderr.writeln('error: the event store refused to open: $e');
    await backend.close();
    await failStartup(e);
    return;
  }

  if (components.policyErrors.isNotEmpty) {
    stderr.writeln(
      'WARN: policy seed validation produced errors. Server will run with '
      'FailSafeAuthorizationPolicy (every dispatch denied):',
    );
    for (final err in components.policyErrors) {
      stderr.writeln('  - $err');
    }
  }

  // The delivery cycle of the server's database. Several server processes
  // may share one Postgres database: one drains, and the others stand by
  // and take over when it stops. The deployment's revision identifier is
  // part of the configuration the drainer declares, so a recovery of a
  // halt for reconfiguration is accepted once a revision with another
  // identifier drains.
  final configurationVersion =
      Platform.environment['DEMO_CONFIGURATION_VERSION'];
  final SyncCycle cycle;
  try {
    cycle = await SyncCycle.start(
      registry: components.destinations,
      configurationVersion:
          configurationVersion == null || configurationVersion.isEmpty
          ? null
          : configurationVersion,
    );
  } on Object catch (e) {
    if (e is! DrainLockConfigurationException && e is! ArgumentError) {
      rethrow;
    }
    stderr.writeln('error: the delivery cycle cannot start: $e');
    await components.eventStore.close();
    await failStartup(e);
    return;
  }

  final routes = DemoRoutes(
    components: components,
    projection: PollingDemoStateProjection(
      components: components,
      lastTraceProvider: () => null,
    ),
    deliveryState: () => cycle.state,
  );
  host.serve(routes);
  stdout.writeln('demo server ready');
  stdout.writeln('  backend: $backendDescription');
  stdout.writeln('  data dir: ${dataDir.path}');
  stdout.writeln('  install id: $installId');

  var shuttingDown = false;

  // A delivery cycle that stops for good means this instance can no longer
  // commit to its database (its backend was fenced: a build of another
  // major registered while this instance's lock session was lost). The
  // watch marks the server failed; the process then stops listening and
  // exits non-zero, so the platform's probes fail and it replaces the
  // instance.
  Future<void> stopFenced(Object cause) async {
    if (shuttingDown) return;
    shuttingDown = true;
    stderr.writeln('error: the delivery cycle stopped for good: $cause');
    await host.close();
    try {
      await components.eventStore.close().timeout(const Duration(seconds: 10));
    } on Object catch (e) {
      // A fenced backend refuses its transactions; the exit status reports
      // the stop either way.
      stderr.writeln('closing the event store failed: $e');
    }
    exit(1);
  }

  // On SIGINT or SIGTERM: stop serving, close the delivery cycle (it
  // releases the drain lock, so a standby process takes over), then close
  // the event store and its backend. The handlers are installed before the
  // cycle's state is reported, so a signal that follows the report is
  // always handled.
  Timer? cycleWatch;
  Future<void> shutdown(ProcessSignal signal) async {
    if (shuttingDown) return;
    shuttingDown = true;
    stdout.writeln('demo server stopping ($signal)');
    cycleWatch?.cancel();
    await host.close();
    await cycle.close(timeout: const Duration(seconds: 10));
    await components.eventStore.close();
    exit(0);
  }

  ProcessSignal.sigint.watch().listen(shutdown);
  if (!Platform.isWindows) ProcessSignal.sigterm.watch().listen(shutdown);

  cycleWatch = host.watchCycle(
    cycle,
    log: stdout.writeln,
    onStoppedForGood: stopFenced,
  );
}

Directory _resolveDataDir(String? overridePath) {
  if (overridePath != null) return Directory(overridePath);
  // XDG Base Directory: $XDG_DATA_HOME or ~/.local/share.
  final xdg = Platform.environment['XDG_DATA_HOME'];
  final base = (xdg != null && xdg.isNotEmpty)
      ? xdg
      : p.join(
          Platform.environment['HOME'] ?? Directory.current.path,
          '.local',
          'share',
        );
  return Directory(p.join(base, 'action_permissions_demo'));
}

Future<String> _resolveInstallId(
  Directory dataDir, {
  required bool ephemeral,
}) async {
  if (ephemeral) {
    // No persistence: generate per boot. Documented in --install-id help.
    return _newInstallId();
  }
  final file = File(p.join(dataDir.path, 'install_id'));
  if (await file.exists()) {
    final existing = (await file.readAsString()).trim();
    if (existing.isNotEmpty) return existing;
  }
  final fresh = _newInstallId();
  await file.writeAsString(fresh);
  return fresh;
}

String _newInstallId() {
  // Cheap UUIDv4-shaped string for the demo. For production, use
  // package:uuid. We avoid pulling that dep into bin/ to keep the entry
  // point small; the dispatcher uses it internally for invocation ids.
  final r = DateTime.now().microsecondsSinceEpoch
      .toRadixString(16)
      .padLeft(16, '0');
  return '00000000-0000-4000-8000-${r.substring(r.length - 12).padLeft(12, '0')}';
}

/// True for the refusals with which provisioning and `EventStore.open`
/// decline a database; the server reports them and exits non-zero.
bool _isRefusal(Object e) =>
    e is IncompatibleGenerationException ||
    e is GenerationGuardConfigurationException ||
    e is GenerationFencedException ||
    e is PostgresSchemaIncompatibleException ||
    e is LockSessionConfigurationException ||
    e is DataFormatIncompatibleError ||
    e is EntryTypeVersionDowngradeError ||
    e is DatabaseResetRequiredError ||
    e is DatabaseIdentityMismatchError;
