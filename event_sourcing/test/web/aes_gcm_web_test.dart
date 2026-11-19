// The library's AES-GCM in the browser, where its 32-bit word arithmetic
// runs on JavaScript numbers; see test/storage/aes_gcm_cases.dart.
@TestOn('browser')
library;

import 'package:flutter_test/flutter_test.dart';

import '../storage/aes_gcm_cases.dart';

void main() => defineAesGcmCases();
