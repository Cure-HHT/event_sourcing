// test/action_catalog_test.dart
import 'package:action_permissions_demo/server/action_catalog.dart';
import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('buildDemoActionRegistry', () {
    test('registers all 7 demo actions without collision', () {
      final registry = buildDemoActionRegistry(directory: UserDirectory());
      final names = registry.all.map((a) => a.name).toSet();
      expect(names, <String>{
        'RequestHelpAction',
        'EditGreenNoteAction',
        'EditBlueNoteAction',
        'PressGreenButtonAction',
        'PressBlueButtonAction',
        'PressRedAlarmAction',
        'ProvisionUserAction',
      });
    });

    test('union of declared permissions matches the spec', () {
      final registry = buildDemoActionRegistry(directory: UserDirectory());
      final names = registry.allDeclaredPermissions.map((p) => p.name).toSet();
      expect(names, <String>{
        'help.ask',
        'notes.write.green',
        'notes.write.blue',
        'buttons.press.green',
        'buttons.press.blue',
        'buttons.press.red',
        'users.provision',
      });
    });
  });
}
