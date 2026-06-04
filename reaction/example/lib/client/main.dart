// reaction/example/lib/client/main.dart
//
// Flutter entry point for the reaction example client.
// Run with: `flutter run -d linux -t lib/client/main.dart`

import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:flutter/semantics.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction_example/client/app.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // On web, the accessibility/semantics tree is off by default (a
  // performance optimization). Force-enable it so the DOM exposes
  // flt-semantics nodes for UI automation (Playwright). Production apps
  // may gate this behind a flag; the demo always enables it.
  if (kIsWeb) {
    SemanticsBinding.instance.ensureSemantics();
  }
  runApp(const NotesApp());
}
