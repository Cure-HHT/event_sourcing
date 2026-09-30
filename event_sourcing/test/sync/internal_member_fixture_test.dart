// Verifies: EVS-PRD-destinations/K
// Verifies: EVS-DEV-event-store-open/A
// the test-only EventStore.openForTest, called from a consumer's
//   production code, is reported by the analyzer.
//
// Out-of-package analysis: a consumer package that depends on
// event_sourcing by path, with `invalid_use_of_internal_member` as an
// error, is reported for every use of an internal member -- on the
// barrel's types and through a `src/` import alike -- and not for the
// public reads, transaction and close operations. A member private to its
// Dart library is not visible to the consumer at all. A call of the test-only
// EventStore.openForTest from production code is reported as well. A third-party backend,
// in a package of its own, that overrides every contract member and marks
// each override of an internal member internal analyzes clean, and a
// consumer's call through it is reported; a call through an override that
// carries no annotation is not reported, which is the residual the storage
// dartdoc names. With a cold pub cache the test skips locally and fails
// under CI.
@Timeout(Duration(minutes: 5))
library;

import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

import '../test_support/tool_subprocess.dart';

const _internalUse = <String>{'invalid_use_of_internal_member'};

/// Consumer fixture file -> the diagnostic codes its analysis must report,
/// as a set (empty = analyzes clean).
const _expected = <String, Set<String>>{
  'calls_enqueue_fifo_internal.dart': _internalUse,
  'calls_upsert_view_row_internal.dart': _internalUse,
  'calls_drain_internal.dart': _internalUse,
  // The test-only wedge entry point is visible for testing as well.
  'calls_wedge_head_in_txn_internal.dart': <String>{
    'invalid_use_of_internal_member',
    'invalid_use_of_visible_for_testing_member',
  },
  // Members private to their Dart library: the consumer cannot name them.
  'calls_library_private_members.dart': <String>{
    'undefined_getter',
    'undefined_setter',
    'undefined_method',
  },
  'calls_fill_batch_internal.dart': _internalUse,
  'calls_drain_lock_internal.dart': _internalUse,
  'calls_historical_replay_internal.dart': _internalUse,
  'calls_test_hooks_internal.dart': _internalUse,
  'calls_pool_internal.dart': _internalUse,
  // The test-support accessor is also visible-for-testing only.
  'calls_database_for_testing_internal.dart': <String>{
    'invalid_use_of_internal_member',
    'invalid_use_of_visible_for_testing_member',
  },
  'calls_security_context_internal.dart': _internalUse,
  'calls_third_party_internal.dart': _internalUse,
  // The test-only open, called from production code.
  'calls_test_only_open.dart': <String>{
    'invalid_use_of_visible_for_testing_member',
  },
  'calls_unguarded_third_party.dart': <String>{},
  'calls_transaction_and_close.dart': <String>{},
};

/// Minimum number of `invalid_use_of_internal_member` reports per file.
const _minimumInternalUses = <String, int>{
  'calls_enqueue_fifo_internal.dart': 1,
  'calls_upsert_view_row_internal.dart': 1,
  // drain and honourHaltById.
  'calls_drain_internal.dart': 2,
  // DestinationRegistry.eventStore and wedgeHeadInTxnForTest.
  'calls_wedge_head_in_txn_internal.dart': 2,
  'calls_fill_batch_internal.dart': 1,
  // tryAcquireDrainLock, requestDrainLock, writeRefillGuardTxn and
  // PostgresBackend.whenRegistered.
  'calls_drain_lock_internal.dart': 4,
  // buildHistoricalReplayRows and writeQueueItemsTxn.
  'calls_historical_replay_internal.dart': 2,
  // runWithDeliveryTestHooks and the DeliveryTestHooks constructor.
  'calls_test_hooks_internal.dart': 2,
  'calls_pool_internal.dart': 1,
  'calls_database_for_testing_internal.dart': 1,
  // The abstract store's deleteInTxn and the concrete store's override.
  'calls_security_context_internal.dart': 2,
  'calls_third_party_internal.dart': 1,
};

/// One machine-format diagnostic line: file name and error code.
typedef _Diagnostic = ({String file, String code, String line});

/// Copies the fixture package [name] into [into], with a pubspec naming
/// [pathDependencies] by absolute path and [hostedDependencies] at any
/// version the offline cache holds.
Future<Directory> _stagePackage(
  String name,
  Directory into,
  Map<String, String> pathDependencies, {
  List<String> hostedDependencies = const <String>[],
}) async {
  final fixture = Directory(
    p.join(Directory.current.path, 'test', 'fixtures', name),
  );
  final staged = Directory(p.join(into.path, name));
  for (final f in fixture.listSync(recursive: true).whereType<File>()) {
    final relative = p.relative(f.path, from: fixture.path);
    if (relative == 'README.md') continue;
    final target = File(p.join(staged.path, relative));
    await target.parent.create(recursive: true);
    await f.copy(target.path);
  }
  final pubspec = StringBuffer()
    ..writeln('name: $name')
    ..writeln('publish_to: none')
    ..writeln('environment:')
    ..writeln('  sdk: ^3.10.7')
    ..writeln('dependencies:');
  pathDependencies.forEach((dep, path) {
    pubspec
      ..writeln('  $dep:')
      ..writeln('    path: ${jsonEncode(path)}');
  });
  for (final dep in hostedDependencies) {
    pubspec.writeln('  $dep: any');
  }
  await File(
    p.join(staged.path, 'pubspec.yaml'),
  ).writeAsString(pubspec.toString());
  return staged;
}

Future<void> _pubGet(Directory package) async {
  final get = await runSdkTool('flutter', <String>[
    'pub',
    'get',
    '--offline',
  ], workingDirectory: package.path);
  if (get.exitCode != 0) {
    throw ToolUnavailable(
      'flutter pub get --offline failed (cold pub cache?):\n'
      '${get.stdout}\n${get.stderr}',
    );
  }
}

Future<List<_Diagnostic>> _analyze(Directory package) async {
  final analyze = await runSdkTool('dart', <String>[
    'analyze',
    '--format=machine',
    'lib',
  ], workingDirectory: package.path);
  final diagnostics = <_Diagnostic>[];
  for (final line in LineSplitter.split('${analyze.stderr}${analyze.stdout}')) {
    final parts = line.split('|');
    if (parts.length < 8) continue;
    diagnostics.add((
      file: p.basename(parts[3]),
      code: parts[2].toLowerCase(),
      line: line,
    ));
  }
  if (analyze.exitCode != 0 && diagnostics.isEmpty) {
    fail(
      'dart analyze failed without diagnostics:\n'
      '${analyze.stdout}\n${analyze.stderr}',
    );
  }
  return diagnostics;
}

typedef _Findings = ({
  List<_Diagnostic> thirdParty,
  List<_Diagnostic> consumer,
});

/// Analyzes the third-party backend package and the consumer package that
/// depends on it.
Future<_Findings> _analyzeFixtures() async {
  final libraryRoot = Directory.current.path;
  final temp = await Directory.systemTemp.createTemp('internal_member_');
  try {
    final thirdParty = await _stagePackage(
      'third_party_backend',
      temp,
      <String, String>{'event_sourcing': libraryRoot},
      hostedDependencies: const <String>['meta'],
    );
    final consumer = await _stagePackage(
      'internal_member_consumer',
      temp,
      <String, String>{
        'event_sourcing': libraryRoot,
        'third_party_backend': thirdParty.path,
      },
    );
    await _pubGet(thirdParty);
    await _pubGet(consumer);
    return (
      thirdParty: await _analyze(thirdParty),
      consumer: await _analyze(consumer),
    );
  } finally {
    await temp.delete(recursive: true);
  }
}

void main() {
  test('a consumer package is reported for each internal use, through the '
      "barrel, a src import or a third-party backend's annotated override, "
      'and not for the public reads, transaction or close', () async {
    final _Findings found;
    try {
      found = await _analyzeFixtures();
    } on ToolUnavailable catch (e) {
      if (runningInCi) fail('$e');
      markTestSkipped('$e');
      return;
    }
    expect(
      found.thirdParty,
      isEmpty,
      reason:
          'a third-party backend overriding every member, with @internal on '
          'each override of an internal member, analyzes clean: '
          '${found.thirdParty.map((d) => d.line).join('\n')}',
    );
    final byFile = <String, List<_Diagnostic>>{};
    for (final d in found.consumer) {
      (byFile[d.file] ??= <_Diagnostic>[]).add(d);
    }
    expect(
      byFile.keys.toSet().difference(_expected.keys.toSet()),
      isEmpty,
      reason: 'diagnostics in unexpected files: ${found.consumer}',
    );
    _expected.forEach((file, codes) {
      final inFile = byFile[file] ?? const <_Diagnostic>[];
      expect(
        inFile.map((d) => d.code).toSet(),
        codes,
        reason: '$file: ${inFile.map((d) => d.line).join('\n')}',
      );
      final minimum = _minimumInternalUses[file];
      if (minimum != null) {
        expect(
          inFile.where((d) => d.code == 'invalid_use_of_internal_member'),
          hasLength(greaterThanOrEqualTo(minimum)),
          reason: file,
        );
      }
    });
  });
}
