// reaction/example/test_driver/app_test.dart
//
// Tier-2 UI confirmation: drives the REAL Flutter desktop client through
// flutter_driver while a SECOND client (bob) acts over plain HTTP against
// the same server. Confirms the reaction_widgets UI reflects another
// client's write live — the multi-client reactive path, end to end
// through the rendered UI.
//
// The UI client logs in as `dave` (a viewer): a viewer's home screen has
// no admin panel, so the notes ListView gets enough vertical space to
// build its first row in the (small) headless driver window. Logging in
// as an admin (carol) collapses the notes Expanded below the panels in a
// short window, so the lazily-built ListView builds no items and the row,
// though present in widget state (Ready), is not laid out — a window-size
// artifact of the demo's single-Expanded layout, not a reactive defect.
//
// Run (with a demo server already listening on $REACTION_SERVER_URL,
// default http://127.0.0.1:8080):
//
//   dart run bin/server.dart --port 8080 &
//   flutter drive \
//     --driver=test_driver/app_test.dart \
//     --target=lib/client/driver_main.dart \
//     -d linux
//
// This file is the `--driver` side: it runs in a separate VM and talks
// to the app the `--target` side launches.

import 'dart:convert';

import 'package:flutter_driver/flutter_driver.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

void main() {
  const baseUrl = String.fromEnvironment(
    'REACTION_SERVER_URL',
    defaultValue: 'http://127.0.0.1:8080',
  );

  group('reaction_example UI (multi-client reactive)', () {
    late FlutterDriver driver;

    setUpAll(() async {
      driver = await FlutterDriver.connect();
      await driver.waitUntilFirstFrameRasterized();
    });

    tearDownAll(() async {
      await driver.close();
    });

    test(
      'a note posted by another client over HTTP appears live in the UI',
      () async {
        // --- Log in as dave (viewer) through the real LoginScreen. ---
        final field = find.byType('TextField');
        await driver.waitFor(field);
        await driver.tap(field);
        await driver.enterText('dave');
        // Tap the FilledButton (not the "Sign in" header Text, which is
        // ambiguous).
        await driver.tap(find.byType('FilledButton'));

        // Home screen settles: the notes list shows the empty-state.
        await driver.waitFor(
          find.text('(no notes yet)'),
          timeout: const Duration(seconds: 15),
        );

        // --- A DIFFERENT client (bob) writes a note over HTTP. ---
        final title = 'from-bob-ui-${DateTime.now().millisecondsSinceEpoch}';
        final res = await http.post(
          Uri.parse('$baseUrl/actions'),
          headers: const {
            'Authorization': 'Bearer bob',
            'Content-Type': 'application/json',
          },
          body: jsonEncode({
            'actionName': 'submit_note',
            'rawInput': {'workspace': 'east', 'title': title},
          }),
        );
        expect(res.statusCode, 200, reason: 'bob POST /actions -> ${res.body}');

        // --- dave's UI reflects bob's write live, with no interaction. ---
        await driver.waitFor(
          find.text(title),
          timeout: const Duration(seconds: 15),
        );
      },
    );
  });
}
