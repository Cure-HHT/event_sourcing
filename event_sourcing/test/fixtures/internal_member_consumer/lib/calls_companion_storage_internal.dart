import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/storage_description.dart';

/// Hands an application's own backend to `EventStore.open` as storage the
/// library opened, by extending the companion-backend description.
final class OwnBackendAsLibraryStorage extends CompanionBackendStorage {
  const OwnBackendAsLibraryStorage(this.backend, this.securityContexts);

  final StorageBackend backend;
  final MutableSecurityContextStore securityContexts;

  @override
  Future<(StorageBackend, MutableSecurityContextStore)> openBackend() async =>
      (backend, securityContexts);
}

/// Builds the idempotency store over a backend directly.
Object? idempotencyStoreOf(StorageBackend backend) =>
    backend.idempotencyStoreOverThis();
