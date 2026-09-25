import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// Installs the library's test seams through a `src/` import.
void installSeams(void Function() body) =>
    runWithDeliveryTestHooks(const DeliveryTestHooks(), body);
