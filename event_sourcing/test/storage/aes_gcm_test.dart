// The library's AES-GCM on the VM, checked against the GCM specification's
// known answers and against pointycastle's independent GCM implementation
// (aes_gcm_cases.dart).
import 'package:flutter_test/flutter_test.dart';

import 'aes_gcm_cases.dart';

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
