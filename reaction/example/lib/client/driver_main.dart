// reaction/example/lib/client/driver_main.dart
//
// flutter_driver-enabled entry point, identical to `main.dart` except it
// calls `enableFlutterDriverExtension()` before `runApp`. Used by Tier-2
// UI confirmation runs that drive the real desktop/web client via the
// Dart/Flutter MCP `flutter_driver` tool (or `flutter drive`).
//
// Run with: `flutter run -d linux -t lib/client/driver_main.dart`
//
// Kept out of `main.dart` so production/demo launches never carry the
// driver extension.

import 'package:flutter/widgets.dart';
import 'package:flutter_driver/driver_extension.dart';
import 'package:reaction_example/client/app.dart';

void main() {
  enableFlutterDriverExtension();
  runApp(const NotesApp());
}
