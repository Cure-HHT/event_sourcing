part of '../event_store.dart';

/// The receiver endpoint of an event store: it accepts the native
/// deliveries (`esd/batch@3`) senders present on their delivery channels,
/// and answers each with the receiver's record of the channel.
///
/// The event store builds its one endpoint ([EventStore.receiverEndpoint]);
/// nothing builds one from a storage backend.
// Implements: EVS-PRD-delivery-channel/P
// the library's receiver endpoint accepts deliveries, authenticated by the
//   sender identities the deployment grants the caller.
final class ReceiverEndpoint {
  ReceiverEndpoint._(this._store);

  final EventStore _store;

  /// Accepts [bytes] as a native delivery presented by a caller that the
  /// deployment's authentication states may act for the sending databases
  /// [senderDatabaseIds], and returns the receiver's answer: an
  /// acknowledgement or a refusal, each carrying the receiver's record of
  /// the channel (the number and hash of the last delivery it accepted on
  /// it, read from the accepted-delivery audits it authored).
  ///
  /// In this order:
  ///
  /// 1. A delivery whose channel names a sending database outside
  ///    [senderDatabaseIds] throws [DeliveryAuthenticationRefused] before
  ///    any read of the channel and any write.
  /// 2. A batch that does not decode as a delivery is refused `rejected`
  ///    naming the decoder's reason. One from which no channel can be read
  ///    throws the [IngestDecodeFailure] instead, since no refusal can name
  ///    its channel.
  /// 3. A delivery carrying a succession event the receiver does not hold
  ///    throws [DeliveryAuthenticationRefused], as an unauthenticated
  ///    sender, when [senderDatabaseIds] does not name both the event's
  ///    successor and its predecessor. A succession event the receiver
  ///    already holds is a duplicate and no check applies to it.
  /// 4. A delivery whose `delivery_hash` does not recompute is refused
  ///    `delivery_hash_mismatch`, and a `delivery_hash_mismatch` security
  ///    finding is recorded, once, in a transaction that writes nothing
  ///    else. The sender treats it as transient and sends it again.
  /// 5. Inside the ingest transaction, before any write, the record is
  ///    read. A delivery whose number and hash equal it is acknowledged
  ///    `represented` and nothing is appended; any other delivery that is
  ///    not numbered one above it and linked to its hash is refused
  ///    `out_of_sequence`.
  /// 6. Otherwise every event is handled as ingest handles a record (a
  ///    record the library cannot store as an event is kept in a finding,
  ///    every other one is admitted, anomalies are recorded as findings),
  ///    each stored event's receiver entry names the delivery, an event
  ///    whose originator entry or last provenance entry does not name the
  ///    channel's sender is recorded in a `foreign_event` finding, one
  ///    `ingest.delivery_accepted` audit is appended, and the delivery is
  ///    acknowledged `accepted` with the new record.
  ///
  /// An event of another data-format major, or of an entry-type version
  /// the receiver cannot admit, rolls the delivery back and is refused
  /// `rejected` naming the refusal's reason and the event.
  // Implements: EVS-DEV-delivery-receiver/N
  // the accept path refuses a delivery whose channel's sender is not in the
  //   caller's sender identities before any read of the channel and any
  //   write.
  // Implements: EVS-DEV-sender-succession/F
  // a delivery carrying a succession event the receiver does not hold is
  //   refused, as an unauthenticated sender, when the caller may not act
  //   for both the successor and the predecessor it names.
  // Implements: EVS-DEV-sender-succession/E
  // a succession event the receiver already holds is handled as any other
  //   held event; no succession check applies to it.
  // Implements: EVS-PRD-delivery-channel/E
  // every acknowledgement and every refusal the endpoint answers with
  //   carries the receiver's record of the channel.
  Future<ReceiverResponse> accept(
    Uint8List bytes, {
    required Set<String> senderDatabaseIds,
  }) async {
    DeliveryEnvelope? delivery;
    IngestDecodeFailure? failure;
    DeliveryChannel? channel;
    try {
      delivery = DeliveryEnvelope.decode(bytes);
      channel = delivery.channel;
    } on IngestDecodeFailure catch (e) {
      failure = e;
      channel = _channelOf(bytes);
      if (channel == null) rethrow;
    }
    if (!senderDatabaseIds.contains(channel.senderDatabaseId)) {
      throw DeliveryAuthenticationRefused(
        senderDatabaseId: channel.senderDatabaseId,
      );
    }
    if (delivery == null) {
      return _refusal(
        channel,
        await _readRecord(channel),
        RefusalKind.rejected,
        reason: failure!.reason,
      );
    }
    await _refuseUnauthenticatedSuccessions(delivery, senderDatabaseIds);
    final recomputed = delivery.recomputedDeliveryHash;
    if (recomputed != delivery.deliveryHash) {
      return _refuseHashMismatch(delivery, recomputed);
    }
    return _ingest(delivery, bytes);
  }

  /// Throws [DeliveryAuthenticationRefused], as an unauthenticated sender,
  /// for the first event of [delivery] that is a succession event
  /// (`system.destination_sender_succeeded`) the receiver does not already
  /// hold and whose `database_id` (the successor) or
  /// `predecessor_database_id` (the predecessor) is outside
  /// [senderDatabaseIds]. A succession event the receiver already holds is
  /// a duplicate and is not checked; nor is a record that does not decode
  /// as a succession event, since ingest's own record handling meets it.
  // Implements: EVS-DEV-sender-succession/F
  // refuses, as it refuses a caller it does not authenticate for a
  //   channel's sender, a delivery carrying a succession event the
  //   receiver does not hold when the caller may not act for both the
  //   successor and the predecessor.
  // Implements: EVS-DEV-sender-succession/E
  // a delivered succession event the receiver already holds is handled as
  //   any event it already holds, with no succession check applied.
  Future<void> _refuseUnauthenticatedSuccessions(
    DeliveryEnvelope delivery,
    Set<String> senderDatabaseIds,
  ) async {
    for (final record in delivery.events) {
      if (record['entry_type'] != kDestinationSenderSucceededEntryType) {
        continue;
      }
      final eventId = record['event_id'];
      if (eventId is! String) continue;
      if (await _store._backend.findEventById(eventId) != null) continue;
      final data = record['data'];
      if (data is! Map<String, Object?>) continue;
      final SenderSuccessionData succession;
      try {
        succession = SenderSuccessionData.fromJson(data);
      } on FormatException {
        continue;
      }
      final unauthenticated = !senderDatabaseIds.contains(succession.databaseId)
          ? succession.databaseId
          : !senderDatabaseIds.contains(succession.predecessorDatabaseId)
          ? succession.predecessorDatabaseId
          : null;
      if (unauthenticated != null) {
        throw DeliveryAuthenticationRefused(senderDatabaseId: unauthenticated);
      }
    }
  }

  /// Serves [request] to a caller that the deployment's authentication
  /// states may act for the sending databases [senderDatabaseIds], reading
  /// the receiver's event log in one transaction that writes nothing.
  ///
  /// A pull naming a sending database outside [senderDatabaseIds] (the
  /// listing's sender, or the range's channel's sender) throws
  /// [DeliveryAuthenticationRefused] before any read and any write.
  ///
  /// A [ChannelListingPull] is answered with a [ChannelListing]: every
  /// channel, of every generation, on which this database accepted a
  /// delivery from the sender or an identity of its succession lineage,
  /// each with the receiver's record of it, ordered by sender, destination,
  /// registration and generation.
  ///
  /// A [DeliveryRangePull] is answered with a [DeliveryRange] carrying the
  /// receiver's record of the channel and, in ascending order, each
  /// delivery of the range the receiver can serve, reconstructed from its
  /// `ingest.delivery_accepted` audit: its number, link, hash and
  /// attributes as the delivery carried them, and, in the order the audit
  /// lists them, the stored record of each event it names. An event the
  /// receiver holds only in a security finding's evidence (it cannot store
  /// it as an event, or holds its identifier under another hash) is served
  /// as that evidence carries it. The first delivery of the range the
  /// receiver cannot serve (one above its record, or one within it for
  /// which it holds no audit, or neither holds as an event nor in a
  /// finding's evidence an event the audit names) ends the range and is
  /// named as unservable.
  // Implements: EVS-PRD-delivery-channel/P
  // the library's receiver endpoint serves, to a caller the deployment
  //   authenticates for the sender, the channels it holds for that sender
  //   with their records and the deliveries of a range of a channel,
  //   reconstructed from its event log.
  // Implements: EVS-DEV-delivery-receiver/N
  // the pull refuses a pull naming a sender outside the caller's sender
  //   identities before any read of a channel and any write.
  Future<PullResponse> pull(
    PullRequest request, {
    required Set<String> senderDatabaseIds,
  }) async {
    final sender = switch (request) {
      ChannelListingPull(:final senderDatabaseId) => senderDatabaseId,
      DeliveryRangePull(:final channel) => channel.senderDatabaseId,
    };
    if (!senderDatabaseIds.contains(sender)) {
      throw DeliveryAuthenticationRefused(senderDatabaseId: sender);
    }
    return _store._runInTxnWithPublish(
      (txn, _) => switch (request) {
        final ChannelListingPull listing => _listInTxn(txn, listing),
        final DeliveryRangePull range => _serveRangeInTxn(txn, range),
      },
    );
  }

  /// The identities whose channels a listing for [senderDatabaseId]
  /// covers: the sender and the identities of its succession lineage, as
  /// the succession events the receiver holds state it, read inside [txn].
  Future<Set<String>> _successionLineageOf(
    Transaction txn,
    String senderDatabaseId,
  ) async => lineageSetOf(
    await computeSuccessionLineageInTxn(txn, _store._backend, senderDatabaseId),
    senderDatabaseId,
  );

  /// The channel listing [request] asks for, read inside [txn].
  // Implements: EVS-DEV-delivery-receiver/R
  // the listing names each channel, of every generation, that the
  //   receiver's log records of the sender and of its succession lineage,
  //   each with the receiver's record of it.
  Future<ChannelListing> _listInTxn(
    Transaction txn,
    ChannelListingPull request,
  ) async {
    final audits = await _store._backend.findLatestAuthoredDeliveryAuditsInTxn(
      txn,
      databaseId: _store.databaseId,
      senderDatabaseIds: await _successionLineageOf(
        txn,
        request.senderDatabaseId,
      ),
    );
    final channels = <ListedChannel>[
      for (final audit in audits)
        ListedChannel(
          channel: DeliveryChannel.fromJson(audit.data['channel']),
          record: _recordOfAudit(audit),
        ),
    ]..sort((a, b) => _compareChannels(a.channel, b.channel));
    return ChannelListing(
      receiverDatabaseId: _store.databaseId,
      senderDatabaseId: request.senderDatabaseId,
      channels: channels,
    );
  }

  static int _compareChannels(DeliveryChannel a, DeliveryChannel b) {
    var c = a.senderDatabaseId.compareTo(b.senderDatabaseId);
    if (c != 0) return c;
    c = a.destinationId.compareTo(b.destinationId);
    if (c != 0) return c;
    c = a.registrationId.compareTo(b.registrationId);
    if (c != 0) return c;
    return a.generation.compareTo(b.generation);
  }

  /// The delivery range [request] asks for, read inside [txn].
  // Implements: EVS-DEV-delivery-receiver/O
  // the pull returns the receiver's identity, its record of the channel
  //   and, for each delivery of the range, its number, link, hash and
  //   attributes and, in its audit's order, the stored record of each event
  //   it names, or the record a finding's evidence carries.
  // Implements: EVS-DEV-delivery-receiver/P
  // the pull names, as one it cannot serve, the first delivery of the
  //   range above its record, or within it lacking an audit or an event the
  //   audit names.
  Future<DeliveryRange> _serveRangeInTxn(
    Transaction txn,
    DeliveryRangePull request,
  ) async {
    final channel = request.channel;
    final record = await _recordInTxn(txn, channel);
    final last = request.toDeliveryNumber < record.deliveryNumber
        ? request.toDeliveryNumber
        : record.deliveryNumber;
    final audits = <int, StoredEvent>{
      if (request.fromDeliveryNumber <= last)
        for (final audit
            in await _store._backend.findAuthoredDeliveryAuditsInTxn(
              txn,
              databaseId: _store.databaseId,
              aggregateId: _auditAggregateId(channel),
              fromDeliveryNumber: request.fromDeliveryNumber,
              toDeliveryNumber: last,
            ))
          (audit.data['delivery_number']! as num).toInt(): audit,
    };
    final evidence = _FindingEvidence(_store, txn);
    final served = <ServedDelivery>[];
    int? unservable;
    for (
      var n = request.fromDeliveryNumber;
      n <= request.toDeliveryNumber;
      n++
    ) {
      final audit = audits[n];
      final events = audit == null
          ? null
          : await _servedEventsInTxn(txn, audit, evidence);
      if (events == null) {
        unservable = n;
        break;
      }
      // Implements: EVS-DEV-delivery-receiver/X
      // the pull serves the attributes object as the delivery carried it,
      //   as the audit keeps it.
      served.add(
        ServedDelivery(
          deliveryNumber: n,
          previousDeliveryHash:
              audit!.data['previous_delivery_hash'] as String?,
          deliveryHash: audit.data['delivery_hash']! as String,
          attributes: (audit.data['attributes']! as Map)
              .cast<String, Object?>(),
          events: events,
        ),
      );
    }
    return DeliveryRange(
      receiverDatabaseId: _store.databaseId,
      channel: channel,
      record: record,
      deliveries: served,
      unservableDeliveryNumber: unservable,
    );
  }

  /// The records of the events [audit] names, in its order, read inside
  /// [txn], or null when the receiver holds one of them neither as an event
  /// nor in a finding's evidence.
  ///
  /// A held event is served when it arrived as the audit lists it (its
  /// event hash, or its last provenance entry's arrival hash, is the hash
  /// the audit lists); otherwise a record a finding's evidence carries
  /// under the listed identifier and hash is served; otherwise the held
  /// event is served as stored.
  Future<List<Map<String, Object?>>?> _servedEventsInTxn(
    Transaction txn,
    StoredEvent audit,
    _FindingEvidence evidence,
  ) async {
    final ids = audit.data['event_ids']! as List;
    final hashes = audit.data['event_hashes']! as List;
    final events = <Map<String, Object?>>[];
    for (var i = 0; i < ids.length; i++) {
      final id = ids[i];
      final hash = hashes[i];
      final held = id is String
          ? await _store._backend.findEventByIdInTxn(txn, id)
          : null;
      if (held != null && _arrivedAs(held, hash)) {
        events.add(held.toMap());
        continue;
      }
      final kept = await evidence.recordOf(id, hash);
      if (kept != null) {
        events.add(kept);
      } else if (held != null) {
        events.add(held.toMap());
      } else {
        return null;
      }
    }
    return events;
  }

  /// Whether [held] is the copy of an event that arrived, or was sealed,
  /// under [hash].
  static bool _arrivedAs(StoredEvent held, Object? hash) {
    if (held.eventHash == hash) return true;
    final provenance = held.metadata['provenance'];
    if (provenance is! List || provenance.isEmpty) return false;
    final last = provenance.last;
    return last is Map && last['arrival_hash'] == hash;
  }

  /// The channel of a batch that did not decode as a delivery, when its
  /// bytes are a JSON object carrying a well-formed `channel`; otherwise
  /// null.
  static DeliveryChannel? _channelOf(Uint8List bytes) {
    try {
      final decoded = jsonDecode(utf8.decode(bytes));
      if (decoded is! Map<String, Object?>) return null;
      return DeliveryChannel.fromJson(decoded['channel']);
    } on FormatException {
      return null;
    }
  }

  /// The aggregate the accepted-delivery audits of [channel] are appended
  /// to: one per channel, named by the digest of the channel's canonical
  /// JSON, so no identifier a sender chooses can make two channels share
  /// it.
  static String _auditAggregateId(DeliveryChannel channel) =>
      'ingest-audit:delivery:'
      '${sha256.convert(canonicalizeBytes(channel.toJson()))}';

  /// The receiver's record of [channel], read inside [txn]: the number and
  /// hash of the `ingest.delivery_accepted` audit of the channel that this
  /// database authored last, or [DeliveryRecord.none].
  ///
  /// The receiver appends an audit only for the delivery that follows its
  /// record, so the authored audits of a channel are appended in delivery
  /// number order and the latest is the one with the highest number. An
  /// audit another database authored, stored here by ingest, never counts.
  // Implements: EVS-DEV-delivery-receiver/I
  // the record is the number and hash of the highest-numbered authored
  //   accepted-delivery audit naming the channel, or number 0 and a null
  //   hash when there is none.
  // Implements: EVS-PRD-delivery-channel/D
  // the receiver's record of a channel is derived from its event log alone.
  Future<DeliveryRecord> _recordInTxn(
    Transaction txn,
    DeliveryChannel channel,
  ) async {
    final audit = await _store._backend.readLatestAuthoredOfAggregateInTxn(
      txn,
      databaseId: _store.databaseId,
      aggregateId: _auditAggregateId(channel),
    );
    if (audit == null ||
        audit.entryType != kIngestAuditEntryType ||
        audit.eventType != kIngestDeliveryAcceptedEventType) {
      return DeliveryRecord.none;
    }
    return _recordOfAudit(audit);
  }

  /// The record an authored `ingest.delivery_accepted` [audit] states: its
  /// delivery number and hash.
  static DeliveryRecord _recordOfAudit(StoredEvent audit) => DeliveryRecord(
    deliveryNumber: (audit.data['delivery_number']! as num).toInt(),
    deliveryHash: audit.data['delivery_hash']! as String,
  );

  /// The receiver's record of [channel], in a transaction that writes
  /// nothing.
  Future<DeliveryRecord> _readRecord(DeliveryChannel channel) =>
      _store._runInTxnWithPublish((txn, _) => _recordInTxn(txn, channel));

  ReceiverRefusal _refusal(
    DeliveryChannel channel,
    DeliveryRecord record,
    RefusalKind refusal, {
    String? reason,
    String? refusedEventId,
  }) => ReceiverRefusal(
    channel: channel,
    receiverDatabaseId: _store.databaseId,
    record: record,
    refusal: refusal,
    reason: reason,
    refusedEventId: refusedEventId,
  );

  ReceiverAcknowledgement _acknowledgement(
    DeliveryChannel channel,
    DeliveryRecord record,
    AcknowledgementOutcome outcome,
  ) => ReceiverAcknowledgement(
    channel: channel,
    receiverDatabaseId: _store.databaseId,
    record: record,
    outcome: outcome,
  );

  /// Records the `delivery_hash_mismatch` finding for [delivery], whose
  /// carried hash does not recompute to [recomputed], and refuses it.
  // Implements: EVS-DEV-delivery-receiver/W
  // a batch whose delivery hash does not recompute is refused
  //   delivery_hash_mismatch, and a delivery_hash_mismatch finding naming the
  //   channel, the number and both hashes is recorded in a transaction that
  //   writes nothing else.
  // Implements: EVS-PRD-delivery-channel/Z
  // a delivery hash that does not recompute is refused only as a transient
  //   failure, with a security finding.
  Future<ReceiverResponse> _refuseHashMismatch(
    DeliveryEnvelope delivery,
    String recomputed,
  ) async {
    final channel = delivery.channel;
    final record = await _store._runInTxnWithPublish((txn, collector) async {
      final record = await _recordInTxn(txn, channel);
      await _store._recordFindingInTxn(
        txn,
        collector,
        role: FindingRole.ingest,
        kind: FindingKind.deliveryHashMismatch,
        evidence: <String, Object?>{
          'channel': channel.toJson(),
          'delivery_number': delivery.deliveryNumber,
          'carried_hash': delivery.deliveryHash,
          'recomputed_hash': recomputed,
        },
        aggregates: const <String>[],
      );
      return record;
    });
    return _refusal(channel, record, RefusalKind.deliveryHashMismatch);
  }

  /// Ingests [delivery], decoded from [bytes], in one transaction, and
  /// answers it.
  Future<ReceiverResponse> _ingest(
    DeliveryEnvelope delivery,
    Uint8List bytes,
  ) async {
    final channel = delivery.channel;
    // The record the latest run of the ingest transaction read, for a
    // refusal that rolls the transaction back.
    var readRecord = DeliveryRecord.none;
    try {
      return await _store._runInTxnWithPublish((txn, collector) async {
        // Implements: EVS-DEV-delivery-receiver/B
        // the record is read inside the ingest transaction before any write.
        final record = await _recordInTxn(txn, channel);
        readRecord = record;
        // Implements: EVS-DEV-delivery-receiver/C
        // a delivery whose number and hash equal the record is acknowledged
        //   and appends no event.
        if (delivery.deliveryNumber == record.deliveryNumber &&
            delivery.deliveryHash == record.deliveryHash) {
          return _acknowledgement(
            channel,
            record,
            AcknowledgementOutcome.represented,
          );
        }
        // Implements: EVS-DEV-delivery-receiver/D
        // every other delivery not numbered one above the record and linked
        //   to its hash is refused out_of_sequence naming the record.
        // Implements: EVS-PRD-delivery-channel/C
        // a delivery is accepted only when it follows the receiver's record
        //   by number and link.
        if (delivery.deliveryNumber != record.deliveryNumber + 1 ||
            delivery.previousDeliveryHash != record.deliveryHash) {
          return _refusal(channel, record, RefusalKind.outOfSequence);
        }
        await _acceptInTxn(txn, collector, delivery, bytes);
        return _acknowledgement(
          channel,
          DeliveryRecord(
            deliveryNumber: delivery.deliveryNumber,
            deliveryHash: delivery.deliveryHash,
          ),
          AcknowledgementOutcome.accepted,
        );
      });
    } on IngestDataFormatIncompatible catch (e) {
      return _refusal(
        channel,
        readRecord,
        RefusalKind.rejected,
        reason: IngestDataFormatIncompatible.refusalReason,
        refusedEventId: e.eventId,
      );
    } on IngestEntryTypeVersionAhead catch (e) {
      return _refusal(
        channel,
        readRecord,
        RefusalKind.rejected,
        reason: IngestEntryTypeVersionAhead.refusalReason,
        refusedEventId: e.eventId,
      );
    } on IngestEntryTypeVersionUnpromotable catch (e) {
      return _refusal(
        channel,
        readRecord,
        RefusalKind.rejected,
        reason: IngestEntryTypeVersionUnpromotable.refusalReason,
        refusedEventId: e.eventId,
      );
    }
  }

  /// Stores every event of [delivery] inside [txn], records the findings
  /// they meet, and appends the delivery's accepted-delivery audit.
  // Implements: EVS-PRD-ingest/G
  // the events of a delivery the channel admits are admitted, each as
  //   ingest handles a record, whatever the integrity checks find.
  Future<void> _acceptInTxn(
    Transaction txn,
    PublishCollector collector,
    DeliveryEnvelope delivery,
    Uint8List bytes,
  ) async {
    final channel = delivery.channel;
    final wireBytesHash = sha256.convert(bytes).toString();
    final provenanceDelivery = ProvenanceDelivery(
      senderDatabaseId: channel.senderDatabaseId,
      destinationId: channel.destinationId,
      registrationId: channel.registrationId,
      generation: channel.generation,
      deliveryNumber: delivery.deliveryNumber,
    );
    final events = delivery.events;
    for (var i = 0; i < events.length; i++) {
      final record = events[i];
      EventStore._refuseOtherDataFormatMajorOfRecord(record);
      await _store._ingestRecordInTxn(
        txn,
        record,
        parsed: null,
        batchContext: BatchContext(
          batchId: delivery.batchId,
          batchPosition: i,
          batchSize: events.length,
          batchWireBytesHash: wireBytesHash,
          batchWireFormat: DeliveryEnvelope.wireFormat,
        ),
        collector: collector,
        delivery: provenanceDelivery,
      );
      await _findForeignEventInTxn(txn, collector, delivery, record);
    }
    // Implements: EVS-DEV-delivery-receiver/G
    // one ingest.delivery_accepted audit per accepted delivery, carrying
    //   exactly database_id, channel, delivery_number, delivery_hash,
    //   previous_delivery_hash, event_ids, event_hashes and attributes.
    // Implements: EVS-DEV-delivery-receiver/X
    // the audit keeps the attributes object as the delivery carried it,
    //   whatever names it holds.
    await _appendRawInternalEventInTxn(
      txn,
      _store._backend,
      databaseId: _store.databaseId,
      hop: _store.source.hopId,
      identifier: _store.source.identifier,
      softwareVersion: _store.source.softwareVersion,
      receivedAt: _store._now(),
      aggregateId: _auditAggregateId(channel),
      aggregateType: kIngestAuditAggregateType,
      entryType: kIngestAuditEntryType,
      entryTypeVersion: _store.entryTypes
          .byId(kIngestAuditEntryType)!
          .registeredVersion,
      eventType: kIngestDeliveryAcceptedEventType,
      data: <String, Object?>{
        'database_id': _store.databaseId,
        'channel': channel.toJson(),
        'delivery_number': delivery.deliveryNumber,
        'delivery_hash': delivery.deliveryHash,
        'previous_delivery_hash': delivery.previousDeliveryHash,
        'event_ids': <Object?>[for (final e in events) e['event_id']],
        'event_hashes': delivery.eventHashes,
        'attributes': delivery.attributes,
      },
      initiator: const AutomationInitiator(service: 'ingest'),
      uuid: _store._uuid,
      collector: collector,
    );
  }

  /// Records a `foreign_event` finding for [record], carried by
  /// [delivery], when its originator entry or its last provenance entry
  /// names a database other than the channel's sender.
  // Implements: EVS-DEV-delivery-receiver/T
  // an event whose originator entry or last provenance entry does not name
  //   the channel's sender is accepted, with a foreign_event finding naming
  //   the channel, the delivery number and the event.
  Future<void> _findForeignEventInTxn(
    Transaction txn,
    PublishCollector collector,
    DeliveryEnvelope delivery,
    Map<String, Object?> record,
  ) async {
    final eventId = record['event_id'];
    if (eventId is! String) return;
    final sender = delivery.channel.senderDatabaseId;
    if (EventStore._originatorDatabaseOfRecord(record) == sender &&
        _lastEntryDatabaseOfRecord(record) == sender) {
      return;
    }
    final held = await _store._backend.findEventByIdInTxn(txn, eventId);
    await _store._recordFindingInTxn(
      txn,
      collector,
      role: FindingRole.ingest,
      kind: FindingKind.foreignEvent,
      evidence: <String, Object?>{
        'channel': delivery.channel.toJson(),
        'delivery_number': delivery.deliveryNumber,
        'event_id': eventId,
        'sealed_hash': EventStore._sealedHashOfRecord(record),
      },
      aggregates: <String>[if (held != null) held.aggregateId],
    );
  }

  /// The database identity [record]'s last provenance entry names, or null
  /// when it names none as a string.
  static String? _lastEntryDatabaseOfRecord(Map<String, Object?> record) {
    final metadata = record['metadata'];
    final provenance = metadata is Map ? metadata['provenance'] : null;
    if (provenance is! List || provenance.isEmpty) return null;
    final last = provenance.last;
    final id = last is Map ? last['database_id'] : null;
    return id is String ? id : null;
  }
}

/// The records the security findings this database authored carry in their
/// evidence, by the identifier and event hash each record carries; read,
/// inside one transaction, once and only when a pull asks for one.
final class _FindingEvidence {
  _FindingEvidence(this._store, this._txn);

  final EventStore _store;
  final Transaction _txn;
  Map<(Object?, Object?), Map<String, Object?>>? _records;

  /// The record a finding's evidence carries with identifier [eventId] and
  /// event hash [eventHash], or null when no finding carries one.
  Future<Map<String, Object?>?> recordOf(
    Object? eventId,
    Object? eventHash,
  ) async {
    final records = _records ??= await _read();
    return records[(eventId, eventHash)];
  }

  Future<Map<(Object?, Object?), Map<String, Object?>>> _read() async {
    final records = <(Object?, Object?), Map<String, Object?>>{};
    for (final finding in await _store._backend.findSecurityFindingsInTxn(
      _txn,
    )) {
      if (!finding.isHeldAsAuthoredBy(_store.databaseId)) continue;
      final evidence = finding.data['evidence'];
      final record = evidence is Map ? evidence['record'] : null;
      if (record is! Map) continue;
      records.putIfAbsent((
        record['event_id'],
        record['event_hash'],
      ), () => record.cast<String, Object?>());
    }
    return records;
  }
}
