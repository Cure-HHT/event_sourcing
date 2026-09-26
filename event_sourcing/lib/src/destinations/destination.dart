// Implements: EVS-PRD-destinations/B+E
// Destination abstract interface:
// declares the per-destination event-selection filter (B) and the
// app-supplied delivery implementation contract (transform + send, E).
import 'package:event_sourcing/src/destinations/receiver_response.dart';
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';

/// One synchronization target — e.g. a primary upstream server, a future
/// analytics backend, etc. — that owns its own FIFO, transform, and send.
///
/// A `Destination` has four responsibilities:
///
/// 1. Declaring *what* it wants via [filter]: a deterministic predicate over
///    `(entry_type, event_type, optional predicate)` that selects which
///    events are enqueued to this destination's FIFO.
/// 2. Declaring *how batches are assembled* via [canAddToBatch] and
///    [maxAccumulateTime]: per-candidate admission plus a hold on
///    single-event batches so destinations that prefer to ship batches of
///    two or more are not prematurely flushed when only one event is
///    available.
/// 3. Declaring *how its bytes look on the wire* via [wireFormat] and
///    [transform]: a pure-function serialization from an in-memory event
///    batch to a single [WirePayload] (bytes + contentType + transformVersion)
///    covering the whole batch.
/// 4. Handing the bytes off via [send], returning a [SendResult] that the
///    drain loop routes to success / retry / exhaust outcomes.
///
/// Destinations are registered at app boot in `DestinationRegistry` and are
/// immutable for the process lifetime. A destination's [id] is the key of
/// its FIFO Sembast store (`fifo_{id}`) and SHALL be stable: changing it
/// later would orphan the store's contents. The typical id is a short
/// slug, e.g. `"primary"` for the primary upstream server.
abstract class Destination {
  const Destination();

  /// Stable identifier — used as the FIFO store suffix (`fifo_{id}`) and as
  /// the key in `DestinationRegistry`. SHALL be unique across the registry
  /// and SHALL NOT change for the lifetime of the store.
  String get id;

  /// Event-selection predicate. An event is enqueued to this destination
  /// iff `filter.matches(event)` returns `true`.
  SubscriptionFilter get filter;

  /// Opaque wire-format identifier such as `"json-v1"` or `"fhir-r4"`.
  /// Every `FifoEntry` enqueued for this destination SHALL carry this
  /// value in its `wire_format` column.
  String get wireFormat;

  /// Upper bound on how long `fillBatch` may hold a single-event batch
  /// before flushing it. A single-event batch SHALL NOT flush until
  /// `now() - batch.first.client_timestamp >= maxAccumulateTime` OR
  /// [canAddToBatch] has already returned `false` for a subsequent
  /// candidate. Destinations that are happy with single-event batches
  /// SHALL return `Duration.zero`.
  ///
  /// The replays a registry operation requests do NOT honor this hold. The
  /// first-activation replay runs at the first fill after the activation
  /// and covers every admitted event in the window up to that fill,
  /// including events appended after the activation; it enqueues all of
  /// them in one pass and holds none. A gap replay (a start date moved
  /// earlier) likewise enqueues its events in one pass. Only the fill's
  /// batches of events past the replayed ones are held.
  Duration get maxAccumulateTime;

  /// Whether this destination permits its deletion through
  /// `DestinationRegistry.deleteDestination`, which removes its schedule,
  /// fill position and pending queue items (the delivered, wedged and
  /// recovered items are kept). The abstract default is `false` because
  /// some destinations carry regulatory audit weight; concrete
  /// destinations that permit deletion SHALL override the getter to `true`
  /// as an explicit opt-in. The latest registration's value is the opt-in
  /// in effect.
  bool get allowHardDelete => false;

  /// Whether this destination consumes the library's canonical batch
  /// format (`esd/batch@2`). When `true`, `fillBatch` skips
  /// [transform] entirely and instead constructs a
  /// `BatchEnvelopeMetadata` from the library's source identity, persisted
  /// on the FIFO row as `envelope_metadata` with `wire_payload: null` and
  /// `wire_format: "esd/batch@2"`. The drain path reconstructs the wire
  /// bytes deterministically via `BatchEnvelope.encode` over the
  /// row's events plus `envelope_metadata`.
  ///
  /// When `false` (the default for 3rd-party destinations such as sponsor
  /// CSV or Rave EDC XML), `fillBatch` invokes [transform] and persists the
  /// resulting [WirePayload] verbatim with `envelope_metadata: null`.
  bool get serializesNatively => false;

  /// The destination's pull from the receiver endpoint it delivers to: a
  /// channel listing of a sender database, or a range of a channel's
  /// deliveries, reported through [decodePullResponse]'s outcomes (a
  /// transport failure as [PullTransient] or [PullPermanent]). A
  /// destination that serializes natively provides it; null (the default)
  /// for a destination that does not implement it.
  // Implements: EVS-DEV-delivery-channel/R
  // a destination's pull operation reports through the pull decoder's
  //   outcomes.
  ChannelPull? get channelPull => null;

  /// Destination-owned batching rule. Invoked by `fillBatch` once per
  /// candidate event under consideration: returning `true` adds
  /// [candidate] to [currentBatch]; returning `false` ends the current
  /// batch (the candidate remains available for the next batch / tick).
  /// The predicate SHALL be deterministic and pure — identical inputs
  /// SHALL produce identical outputs across invocations.
  ///
  /// On the first call of a new batch, [currentBatch] is empty. A
  /// destination that returns `false` for an empty `currentBatch` will
  /// never have any row enqueued — this is a legal configuration (it
  /// means the destination refuses to batch this candidate), but it
  /// silently results in no FIFO row being formed on this tick. Most
  /// destinations SHOULD return `true` when `currentBatch.isEmpty` to
  /// accept at least the first event.
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate);

  /// Pure transform from an event [batch] to its wire payload. Produces
  /// exactly one [WirePayload] covering every event in the batch.
  /// Implementations SHALL be deterministic: identical input batches
  /// SHALL produce byte-identical [WirePayload]s, so the
  /// `transform_version` stamp uniquely identifies the transform that
  /// produced the bytes.
  ///
  /// The batch SHALL be non-empty. Callers (drain / fillBatch) SHALL NOT
  /// invoke `transform` with an empty list. Implementations SHALL throw
  /// [ArgumentError] on that precondition violation, as defense-in-depth
  /// for callers that mistakenly pass `[]` — a silent empty-bytes payload
  /// would corrupt the FIFO row's audit semantics.
  ///
  /// The method is `async` because real destinations may do per-batch
  /// async work (e.g., signing, key-store lookup); pure-Dart test doubles
  /// return `Future.value(...)`.
  ///
  /// The returned payload is typically handed directly to `send(...)` on
  /// the drain path; the same bytes are also persisted on the
  /// [`FifoEntry.wirePayload`] for later retry attempts.
  Future<WirePayload> transform(List<StoredEvent> batch);

  /// Hand [payload] to the destination and categorize the outcome.
  ///
  /// Implementations SHALL return one of:
  ///
  /// - [SendOk] — the payload was accepted; drain loop marks the entry sent.
  /// - [SendTransient] — retryable failure (typically 5xx, timeouts, network
  ///   errors); drain loop applies backoff per `SyncPolicy`.
  /// - [SendPermanent] — non-retryable failure (typically 4xx excluding
  ///   rate-limits); drain loop marks the entry wedged and wedges the
  ///   FIFO
  ///
  /// How underlying HTTP codes, network errors, and timeouts map into those
  /// three variants is a per-destination judgment, not dictated by the
  /// contract.
  Future<SendResult> send(WirePayload payload);
}
