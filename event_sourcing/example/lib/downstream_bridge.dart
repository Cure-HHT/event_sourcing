import 'package:event_sourcing/event_sourcing.dart';

/// In-memory bridge from one datastore's outgoing `Native` wire payload
/// to another datastore's [EventStore.ingestBatch]. Demo-only glue used
/// by the dual-pane example to wire the mobile pane's outgoing native
/// stream into the hub pane.
///
/// Maps [EventStore.ingestBatch] outcomes to [SendResult]:
/// - success ([IngestBatchResult]) → [SendOk] (per-event partial outcomes
///   are the receiver's concern, observable on the receiver's audit panel;
///   a record with an integrity anomaly, such as a hash that does not
///   recompute or a reserved entry type the library does not declare, is
///   recorded by the receiver as a security finding and the rest of the
///   batch is admitted)
/// - [IngestDecodeFailure] → [SendPermanent] (won't fix on retry)
/// - [IngestDataFormatIncompatible] / [IngestEntryTypeVersionAhead] /
///   [IngestEntryTypeVersionUnpromotable] → [SendPermanent] (the receiver
///   reads no other data-format major, an entry-type major above its
///   registered one needs a registry upgrade, and a lower version its
///   promoter steps do not lead from needs a registered step; a retry
///   cannot succeed until an operator changes a build)
/// - any other thrown exception → [SendTransient] (treat unknowns as
///   recoverable so drain retries on the next tick)
class DownstreamBridge {
  const DownstreamBridge(this._target);
  final EventStore _target;

  Future<SendResult> deliver(WirePayload payload) async {
    try {
      await _target.ingestBatch(payload.bytes, wireFormat: payload.contentType);
      return const SendOk();
    } on IngestDecodeFailure catch (e) {
      return SendPermanent(error: e.toString());
    } on IngestDataFormatIncompatible catch (e) {
      return SendPermanent(error: e.toString());
    } on IngestEntryTypeVersionAhead catch (e) {
      return SendPermanent(error: e.toString());
    } on IngestEntryTypeVersionUnpromotable catch (e) {
      return SendPermanent(error: e.toString());
    } catch (e) {
      return SendTransient(error: e.toString());
    }
  }
}
