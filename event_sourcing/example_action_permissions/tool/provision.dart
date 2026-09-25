// Runs the demo's Postgres deployment step and exits: the same as
// `dart run bin/server.dart --backend=postgres --provision <options>`.
//
//   dart run tool/provision.dart \
//     --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
//     --postgres-ssl-mode=disable \
//     --postgres-runtime-role=evs_runtime --postgres-lock-role=evs_runtime
import '../bin/server.dart' as server;

Future<void> main(List<String> args) =>
    server.main(<String>['--backend=postgres', '--provision', ...args]);
