// Helpers for tests that run the Dart or Flutter tool in a subprocess.
import 'dart:io';

import 'package:path/path.dart' as p;

/// True when the test runs under continuous integration, where a test that
/// cannot run its subprocess fails instead of skipping.
bool get runningInCi {
  final ci = Platform.environment['CI'];
  return ci != null && ci.isNotEmpty && ci != 'false';
}

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
