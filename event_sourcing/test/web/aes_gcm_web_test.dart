// The library's AES-GCM in the browser, where its 32-bit word arithmetic
// runs on JavaScript numbers, checked as on the VM
// (test/storage/aes_gcm_cases.dart).
@TestOn('browser')
library;

import 'package:flutter_test/flutter_test.dart';

import '../storage/aes_gcm_cases.dart';

void main() {
  // Verifies: EVS-DEV-sembast-encryption/C
  test(
    'meets the GCM specification known answers for a 256-bit key',
    checkKnownAnswers,
  );

  // Verifies: EVS-DEV-sembast-encryption/C
  test(
    'matches an independent GCM implementation at every length',
    checkAgainstReference,
  );

  // Verifies: EVS-DEV-sembast-encryption/C
  test(
    'refuses an altered ciphertext, tag or nonce, and another key',
    checkRefusals,
  );
}
