// Provisions the demo's Postgres schema and exits: the same as
// `dart run bin/server.dart --backend=postgres --provision <options>`.
//
//   dart run tool/provision.dart \
//     --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
//     --postgres-ssl-mode=disable
import '../bin/server.dart' as server;

Future<void> main(List<String> args) =>
    server.main(<String>['--backend=postgres', '--provision', ...args]);
