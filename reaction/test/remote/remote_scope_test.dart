import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_scope.dart';

void main() {
  test('constructs four Remote* impls with shared connection', () async {
    final scope = RemoteScope(baseUrl: Uri.parse('http://localhost:0'));
    expect(scope.authSession, isNotNull);
    expect(scope.actionSubmitter, isNotNull);
    expect(scope.viewSource, isNotNull);
    expect(scope.permissionSource, isNotNull);
    await expectLater(scope.dispose(), completes);
  });
}
