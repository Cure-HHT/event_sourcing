// Helpers for tests that run the Dart or Flutter tool in a subprocess.
import 'dart:io';

import 'package:path/path.dart' as p;

/// True when a test whose tool prerequisite is missing fails instead of
/// skipping. It is true under continuous integration (`CI`) and in a run that
/// records the Evidence Snapshot (`EVS_REQUIRE_PREREQUISITES`), so that both
/// runs record the same outcome for such a test.
bool get prerequisitesRequired =>
    _isSet(Platform.environment['CI']) ||
    _isSet(Platform.environment['EVS_REQUIRE_PREREQUISITES']);

bool _isSet(String? value) =>
    value != null && value.isNotEmpty && value != 'false';

/// Path of the `flutter` or `dart` tool of the SDK running this test.
String sdkTool(String name) {
  final flutterRoot = Platform.environment['FLUTTER_ROOT'];
  if (flutterRoot != null && flutterRoot.isNotEmpty) {
    return p.join(flutterRoot, 'bin', name);
  }
  return name;
}

/// Runs [tool] with [args] in [workingDirectory] and returns the result.
Future<ProcessResult> runSdkTool(
  String tool,
  List<String> args, {
  required String workingDirectory,
}) => Process.run(
  sdkTool(tool),
  args,
  workingDirectory: workingDirectory,
  environment: const <String, String>{'PUB_ENVIRONMENT': 'event_sourcing_test'},
);

/// Thrown when a prerequisite of a subprocess test is missing locally; the
/// caller skips the test, or fails it under continuous integration.
class ToolUnavailable implements Exception {
  ToolUnavailable(this.reason);
  final String reason;
  @override
  String toString() => 'ToolUnavailable: $reason';
}
