// Implements: EVS-DEV-sender-succession/A
// (the restore operation pulls the channels a receiver lists for a
//   predecessor identity and each listed channel's deliveries from 1 up to
//   the receiver's record)
// Implements: EVS-DEV-sender-succession/B
// (every pulled delivery is checked for its link and hash, and every event
//   it stores for its originator entry and its last provenance entry; a
//   failed check is a restore_unverified finding, under detector role
//   restore, and never stops the store)
// Implements: EVS-DEV-sender-succession/C
// (every carried event this database does not hold is stored, in one
//   transaction, in lineage order, then ascending origin position, then
//   ascending registration identifier, generation and delivery number of the
//   lowest pulled delivery carrying it)
// Implements: EVS-DEV-sender-succession/D
// (the succession event is appended only in the transaction that stores the
//   predecessor's events)
// Implements: EVS-DEV-sender-succession/H
// (the restore refuses, before storing anything, five named cases; see
//   SuccessionRestoreRefused)
// Implements: EVS-PRD-delivery-channel/R
// (the restore refuses, before storing anything, into a database that has
//   authored an event of an application entry type)
part of '../event_store.dart';

/// One record a restore pulled, with the channel and delivery number it was
/// pulled from: the occurrence a duplicate across channels or generations
/// is resolved to, and the provenance the stored event is given.
class _RestorePulledRecord {
  _RestorePulledRecord({
    required this.record,
    required this.channel,
    required this.deliveryNumber,
  });

  final Map<String, Object?> record;
  final DeliveryChannel channel;
  final int deliveryNumber;

  /// Whether this occurrence's (registration, generation, delivery number)
  /// sorts below [other]'s: the tie-break the restore keeps, of two
  /// occurrences of the same event, as the one whose channel and delivery
  /// its stored provenance names.
  bool _isLowerThan(_RestorePulledRecord other) {
    final byRegistration = channel.registrationId.compareTo(
      other.channel.registrationId,
    );
    if (byRegistration != 0) return byRegistration < 0;
    final byGeneration = channel.generation.compareTo(other.channel.generation);
    if (byGeneration != 0) return byGeneration < 0;
    return deliveryNumber < other.deliveryNumber;
  }
}

/// The library's succession-restore operation, over one [EventStore]:
/// [EventStore.restoreFromReceiver] is its public entry point, and nothing
/// builds one from a storage backend.
// Implements: EVS-DEV-sender-succession/A+C+D
// see EventStore.restoreFromReceiver's annotations for the full mapping;
//   this class is the operation's implementation.
class _SuccessionRestore {
  _SuccessionRestore._(this._store);

  final EventStore _store;

  Future<StoredEvent> _run({
    required DestinationRegistry registry,
    required String destinationId,
    required String predecessorDatabaseId,
    required Initiator initiator,
  }) async {
    // Implements: EVS-DEV-sender-succession/H
    // a restore naming the successor's own identity as the predecessor is
    //   refused before anything else, including the log-precondition scan.
    if (predecessorDatabaseId == _store.databaseId) {
      throw SuccessionRestoreRefused(
        SuccessionRestoreRefused.predecessorIsSelf,
        "restore: predecessorDatabaseId names the successor's own "
        'identity ($predecessorDatabaseId)',
      );
    }

    // Implements: EVS-DEV-sender-succession/H
    // Implements: EVS-PRD-delivery-channel/R
    // the log preconditions (no authored application event, no authored
    //   succession event) are checked before the restore pulls anything.
    await _checkLogPreconditions();

    final destination = registry.byId(destinationId);
    if (destination == null) {
      throw ArgumentError.value(
        destinationId,
        'destinationId',
        'is not registered in the given registry',
      );
    }
    final pull = destination.channelPull;
    if (pull == null) {
      throw ArgumentError.value(
        destinationId,
        'destinationId',
        'has no channelPull; only a native destination can be restored '
            'through',
      );
    }
    final registrationId = registry.localRegistrationId(destinationId);
    if (registrationId == null) {
      throw ArgumentError.value(
        destinationId,
        'destinationId',
        'has no local registration id',
      );
    }

    final listing = await _pullChannelListing(pull, predecessorDatabaseId);
    // Implements: EVS-DEV-sender-succession/H
    // a restore for a predecessor the receiver lists no channel for is
    //   refused.
    if (listing.channels.isEmpty) {
      throw SuccessionRestoreRefused(
        SuccessionRestoreRefused.noChannelListed,
        'restore: the receiver lists no channel for $predecessorDatabaseId',
      );
    }
    final restoredChannels = <ListedChannel>[
      for (final listed in listing.channels)
        if (listed.record.deliveryNumber > 0) listed,
    ];

    final pulled = <_RestorePulledRecord>[];
    final pulledRanges = <DeliveryRange>[];
    for (final listed in restoredChannels) {
      final range = await _pullDeliveryRange(
        pull,
        listed.channel,
        listed.record.deliveryNumber,
      );
      pulledRanges.add(range);
      for (final delivery in range.deliveries) {
        for (final record in delivery.events) {
          pulled.add(
            _RestorePulledRecord(
              record: record,
              channel: listed.channel,
              deliveryNumber: delivery.deliveryNumber,
            ),
          );
        }
      }
    }

    // Implements: EVS-DEV-sender-succession/C
    // an event served in more than one occurrence (a channel's earlier and
    //   current generation, the ordinary case) is stored once, under the
    //   occurrence whose registration, generation and delivery number sort
    //   lowest. A record this database cannot even read an event_id from is
    //   not deduplicated against anything (there is no reliable key to
    //   dedup it by): every such occurrence is kept, and each reaches
    //   ingest on its own to be recorded as its own event_malformed finding.
    final chosen = <String, _RestorePulledRecord>{};
    var unkeyedCount = 0;
    for (final occurrence in pulled) {
      final eventId = occurrence.record['event_id'];
      if (eventId is! String) {
        chosen['\u0000unkeyed-${unkeyedCount++}'] = occurrence;
        continue;
      }
      final existing = chosen[eventId];
      if (existing == null || occurrence._isLowerThan(existing)) {
        chosen[eventId] = occurrence;
      }
    }

    // Implements: EVS-DEV-sender-succession/C
    // the storing order's lineage component: the predecessor identity's
    //   succession lineage, derived solely from the succession events among
    //   the pulled records (the successor's own log holds none).
    final successions = <SenderSuccessionData>[
      for (final occurrence in chosen.values)
        if (occurrence.record['entry_type'] ==
            kDestinationSenderSucceededEntryType)
          ?_tryParseSuccession(occurrence.record),
    ];
    final lineage = lineageFromSuccessions(successions, predecessorDatabaseId);
    final identityOrder = <String>[
      ...lineage.predecessors,
      predecessorDatabaseId,
    ];
    final identityRank = <String, int>{
      for (var i = 0; i < identityOrder.length; i++) identityOrder[i]: i,
    };

    // A record that does not parse as a well-formed event (the malformed
    // carve-out this ordering step must not crash on; see ingest's
    // event_malformed handling below) has no origin position to rank by.
    // It sorts, within its lineage rank, after every record that does,
    // by the same registration/generation/delivery tie-break as a
    // duplicate occurrence; it is never stored as an event, so its
    // position relative to other unstorable records carries no chain
    // ordering weight.
    final originPositions = <_RestorePulledRecord, int?>{
      for (final occurrence in chosen.values)
        occurrence: _tryOriginPosition(occurrence.record),
    };

    final ordered = chosen.values.toList()
      ..sort((a, b) {
        final rankA =
            identityRank[a.channel.senderDatabaseId] ?? identityRank.length;
        final rankB =
            identityRank[b.channel.senderDatabaseId] ?? identityRank.length;
        if (rankA != rankB) return rankA.compareTo(rankB);
        final posA = originPositions[a];
        final posB = originPositions[b];
        if (posA != null && posB != null) {
          if (posA != posB) return posA.compareTo(posB);
        } else if (posA != null) {
          return -1;
        } else if (posB != null) {
          return 1;
        }
        if (a._isLowerThan(b)) return -1;
        if (b._isLowerThan(a)) return 1;
        return 0;
      });

    await DeliveryTestHooks.current?.beforeRestoreTransaction?.call();

    // Implements: EVS-DEV-sender-succession/C
    // Implements: EVS-DEV-sender-succession/D
    // every restored event, and the succession event that records the
    //   restore, are stored in one transaction.
    return _store._runInTxnWithPublish((txn, collector) async {
      // Implements: EVS-DEV-sender-succession/H
      // Implements: EVS-PRD-delivery-channel/R
      // the log preconditions are checked again inside the storing
      //   transaction, before any write, so an application or succession
      //   event appended after the pre-pull check still refuses the
      //   restore.
      await _checkLogPreconditionsInTxn(txn);
      // Implements: EVS-DEV-sender-succession/B
      // every pulled channel's deliveries are checked for their link and
      //   hash before anything is stored; a failed check is a
      //   restore_unverified finding and never stops the store.
      final receiverIdOf = <DeliveryChannel, String>{
        for (final range in pulledRanges)
          range.channel: range.receiverDatabaseId,
      };
      for (final range in pulledRanges) {
        await _verifyRangeInTxn(txn, collector, range);
      }
      for (final occurrence in ordered) {
        EventStore._refuseOtherDataFormatMajorOfRecord(occurrence.record);
        // Implements: EVS-DEV-security-findings/Q
        // (the restore records every finding this record handling detects
        //   under the detector role restore, not ingest's)
        // Implements: EVS-DEV-security-findings/O
        // (a record the restore cannot store as an event is kept in full in
        //   an event_malformed finding, under role restore, and no event is
        //   stored for it)
        // Implements: EVS-DEV-chain-verification/K
        // (a stored event whose previous_event_hash is the sealed hash of a
        //   held event that its originating database did not author, or of
        //   an event of that database at an origin position not below its
        //   own, is stored as received with a predecessor_break finding)
        // Implements: EVS-DEV-chain-verification/L
        // (a stored event at an origin position a held event of the same
        //   originating database already occupies, or whose predecessor a
        //   held event of that database already carries, is stored as
        //   received with a position_reused or fork_unrecorded finding)
        // Implements: EVS-PRD-hash-chain-integrity/J
        // (every finding the restore records for a failed integrity check
        //   is recorded once per anomaly and detector, in the storing
        //   transaction)
        final outcome = await _store._ingestRecordInTxn(
          txn,
          occurrence.record,
          parsed: null,
          batchContext: null,
          collector: collector,
          delivery: ProvenanceDelivery(
            senderDatabaseId: occurrence.channel.senderDatabaseId,
            destinationId: occurrence.channel.destinationId,
            registrationId: occurrence.channel.registrationId,
            generation: occurrence.channel.generation,
            deliveryNumber: occurrence.deliveryNumber,
          ),
          role: FindingRole.restore,
        );
        // Implements: EVS-DEV-sender-succession/B
        // every event stored is checked for its originator and its last
        //   (receiver) provenance entry; a record the store keeps only in
        //   an event_malformed finding is not an event this database holds,
        //   so it is not checked again here. This runs once, on the
        //   deduplicated occurrence [chosen] picked for storage: a record
        //   carried again in another delivery or generation is a copy of
        //   the same served bytes (the receiver serves one stored record
        //   at each occurrence), so checking only the stored occurrence
        //   checks every distinct record this database was served.
        if (outcome.outcome != IngestOutcome.keptInFinding) {
          await _verifyStoredEventChecksInTxn(
            txn,
            collector,
            occurrence,
            receiverDatabaseId: receiverIdOf[occurrence.channel]!,
          );
        }
      }
      if (DeliveryTestHooks.current?.failRestoreStore?.call() ?? false) {
        throw const InjectedFailure('restore store');
      }
      final successionData = SenderSuccessionData(
        id: destinationId,
        registrationId: registrationId,
        databaseId: _store.databaseId,
        predecessorDatabaseId: predecessorDatabaseId,
        predecessorChannels: <SenderSuccessionChannel>[
          for (final listed in restoredChannels)
            SenderSuccessionChannel(
              channel: listed.channel,
              deliveryNumber: listed.record.deliveryNumber,
              deliveryHash: listed.record.deliveryHash!,
            ),
        ],
      );
      final event = await _store._appendReservedInTxn(
        txn,
        collector,
        entryType: kDestinationSenderSucceededEntryType,
        aggregateId: _store.source.identifier,
        aggregateType: kDestinationAuditAggregateType,
        eventType: kDestinationSenderSucceededEventType,
        data: successionData.toJson(),
        initiator: initiator,
      );
      // Never null: a reserved-entry-type append inside an open transaction
      // always yields the appended event.
      return event!;
    });
  }

  Future<ChannelListing> _pullChannelListing(
    ChannelPull pull,
    String predecessorDatabaseId,
  ) async {
    final outcome = await pull(
      ChannelListingPull(senderDatabaseId: predecessorDatabaseId),
    );
    if (outcome is PullServed && outcome.response is ChannelListing) {
      return outcome.response as ChannelListing;
    }
    throw StateError(
      'restore: the channel listing pull for $predecessorDatabaseId did not '
      'serve a channel listing ($outcome)',
    );
  }

  Future<DeliveryRange> _pullDeliveryRange(
    ChannelPull pull,
    DeliveryChannel channel,
    int toDeliveryNumber,
  ) async {
    final outcome = await pull(
      DeliveryRangePull(
        channel: channel,
        fromDeliveryNumber: 1,
        toDeliveryNumber: toDeliveryNumber,
      ),
    );
    if (outcome is! PullServed || outcome.response is! DeliveryRange) {
      throw StateError(
        'restore: the delivery range pull for $channel did not serve a '
        'delivery range ($outcome)',
      );
    }
    final range = outcome.response as DeliveryRange;
    // Implements: EVS-DEV-sender-succession/H
    // a restore whose pull answers that it cannot serve a delivery it
    //   asked for is refused: the receiver named unservableDeliveryNumber,
    //   or served fewer deliveries than the range asked for.
    final askedCount = toDeliveryNumber; // fromDeliveryNumber is always 1.
    if (range.unservableDeliveryNumber != null ||
        range.deliveries.length < askedCount) {
      throw SuccessionRestoreRefused(
        SuccessionRestoreRefused.deliveryUnservable,
        'restore: the receiver could not serve delivery '
        '${range.unservableDeliveryNumber ?? (range.deliveries.length + 1)} '
        'of $channel',
      );
    }
    return range;
  }

  /// The restore's log preconditions, read outside any transaction, before
  /// the restore pulls anything.
  Future<void> _checkLogPreconditions() async =>
      _refuseIfLogPreconditionsFail(await _store._backend.findAllEvents());

  /// [_checkLogPreconditions]'s check, read inside [txn]: run again just
  /// before the storing transaction writes anything, so a disqualifying
  /// event appended between the pre-pull check and the transaction still
  /// refuses the restore.
  Future<void> _checkLogPreconditionsInTxn(Transaction txn) async =>
      _refuseIfLogPreconditionsFail(
        await _store._backend.findAllEventsInTxn(txn),
      );

  /// Refuses when [events] holds, as authored by this database, an event
  /// of the reserved succession entry type or of any application
  /// (non-reserved) entry type. Reserved events other than a succession
  /// event (the destination registration the restore itself needs, among
  /// others) are not application events and are ignored.
  // Implements: EVS-DEV-sender-succession/H
  // Implements: EVS-PRD-delivery-channel/R
  Future<void> _refuseIfLogPreconditionsFail(List<StoredEvent> events) async {
    for (final event in events) {
      if (!event.isHeldAsAuthoredBy(_store.databaseId)) continue;
      if (event.entryType == kDestinationSenderSucceededEntryType) {
        throw SuccessionRestoreRefused(
          SuccessionRestoreRefused.successionAlreadyAuthored,
          "restore: the successor's log already holds a succession event "
          'it authored (${event.eventId})',
        );
      }
      if (!isReservedEntryType(event.entryType)) {
        throw SuccessionRestoreRefused(
          SuccessionRestoreRefused.applicationEventAuthored,
          "restore: the successor's log already holds an authored event "
          'of application entry type ${event.entryType} (${event.eventId})',
        );
      }
    }
  }

  SenderSuccessionData? _tryParseSuccession(Map<String, Object?> record) {
    final data = record['data'];
    if (data is! Map<String, Object?>) return null;
    try {
      return SenderSuccessionData.fromJson(data);
    } on FormatException {
      return null;
    }
  }

  /// [record]'s origin position, or null when [record] does not parse as a
  /// well-formed event ([StoredEvent.fromMap]'s [FormatException]) or
  /// parses but carries no origin sequence number for a relayed copy
  /// ([StoredEvent.originPosition]'s [StateError]). The ordering step reads
  /// this instead of parsing unguarded, so a record only ingest's own
  /// malformed-record handling can make sense of still reaches storing,
  /// ordered last within its lineage rank, instead of crashing this step.
  static int? _tryOriginPosition(Map<String, Object?> record) {
    try {
      return StoredEvent.fromMap(record, 0).originPosition;
    } on FormatException {
      return null;
      // A served record that does not carry an origin sequence number is
      // untrusted input, the same malformed carve-out the FormatException
      // above covers, not a programming bug.
      // ignore: avoid_catching_errors
    } on StateError {
      return null;
    }
  }

  /// Checks [range]'s deliveries chain by link from delivery 1 and that
  /// each recomputes to its hash; records a `restore_unverified` finding
  /// under role `restore` for each check that fails, inside [txn].
  // Implements: EVS-DEV-sender-succession/B
  Future<void> _verifyRangeInTxn(
    Transaction txn,
    PublishCollector collector,
    DeliveryRange range,
  ) async {
    final channel = range.channel;
    String? expectedLink;
    for (final delivery in range.deliveries) {
      if (delivery.previousDeliveryHash != expectedLink) {
        await _recordRestoreUnverifiedInTxn(
          txn,
          collector,
          channel: channel,
          deliveryNumber: delivery.deliveryNumber,
          eventId: null,
          check: 'delivery_link',
          aggregates: const <String>[],
        );
      }
      // The delivery hash covers each carried event's hash exactly as the
      // sender that built the original delivery hashed it: the receiver's
      // own arrival hash for a record it holds through its own entry, or
      // the record's own event_hash for one it could keep only in a
      // finding's evidence, which carries no receiver entry.
      final eventHashes = <Object?>[
        for (final record in delivery.events)
          _carriedHashOf(record, range.receiverDatabaseId),
      ];
      final recomputed = computeDeliveryHash(
        channel: channel,
        deliveryNumber: delivery.deliveryNumber,
        previousDeliveryHash: delivery.previousDeliveryHash,
        eventHashes: eventHashes,
        attributes: delivery.attributes,
      );
      if (recomputed != delivery.deliveryHash) {
        await _recordRestoreUnverifiedInTxn(
          txn,
          collector,
          channel: channel,
          deliveryNumber: delivery.deliveryNumber,
          eventId: null,
          check: 'delivery_hash',
          aggregates: const <String>[],
        );
      }
      expectedLink = delivery.deliveryHash;
    }
  }

  /// Checks that [occurrence]'s record names its channel's sender in its
  /// originator entry and carries as its last entry
  /// [receiverDatabaseId]'s; records a `restore_unverified` finding under
  /// role `restore`, naming the aggregate of the event just stored, for
  /// each check that fails, inside [txn]. Called only for a record the
  /// store just held as an event.
  // Implements: EVS-DEV-sender-succession/B
  Future<void> _verifyStoredEventChecksInTxn(
    Transaction txn,
    PublishCollector collector,
    _RestorePulledRecord occurrence, {
    required String receiverDatabaseId,
  }) async {
    final record = occurrence.record;
    final rawEventId = record['event_id'];
    final eventId = rawEventId is String ? rawEventId : null;
    final storedAggregateId = eventId == null
        ? null
        : (await _store._backend.findEventByIdInTxn(txn, eventId))?.aggregateId;
    final aggregates = <String>[?storedAggregateId];
    final originator = EventStore._originatorDatabaseOfRecord(record);
    if (originator != occurrence.channel.senderDatabaseId) {
      await _recordRestoreUnverifiedInTxn(
        txn,
        collector,
        channel: occurrence.channel,
        deliveryNumber: occurrence.deliveryNumber,
        eventId: eventId,
        check: 'originator',
        aggregates: aggregates,
      );
    }
    final last = _lastProvenanceEntry(record);
    if (last == null || last['database_id'] != receiverDatabaseId) {
      await _recordRestoreUnverifiedInTxn(
        txn,
        collector,
        channel: occurrence.channel,
        deliveryNumber: occurrence.deliveryNumber,
        eventId: eventId,
        check: 'receiver_entry',
        aggregates: aggregates,
      );
    }
  }

  /// [record]'s last provenance entry, or null when its metadata carries no
  /// well-formed provenance list.
  static Map<String, Object?>? _lastProvenanceEntry(
    Map<String, Object?> record,
  ) {
    final metadata = record['metadata'];
    if (metadata is! Map) return null;
    final provenance = metadata['provenance'];
    if (provenance is! List || provenance.isEmpty) return null;
    final last = provenance.last;
    return last is Map ? Map<String, Object?>.from(last) : null;
  }

  /// The hash [record] carried when a delivery of [receiverDatabaseId]'s
  /// listed it: its last entry's arrival hash when that entry is
  /// [receiverDatabaseId]'s own, otherwise the record's own `event_hash`
  /// (the shape a record kept only in a finding's evidence carries, with no
  /// receiver entry of its own).
  static Object? _carriedHashOf(
    Map<String, Object?> record,
    String receiverDatabaseId,
  ) {
    final last = _lastProvenanceEntry(record);
    if (last != null && last['database_id'] == receiverDatabaseId) {
      return last['arrival_hash'];
    }
    return record['event_hash'];
  }

  // Implements: EVS-DEV-security-findings/Q
  // (recorded under the detector role restore)
  // Implements: EVS-DEV-security-findings/R
  // (evidence carries exactly channel, delivery_number, event_id, null for
  //   a check that concerns the delivery, and check)
  Future<void> _recordRestoreUnverifiedInTxn(
    Transaction txn,
    PublishCollector collector, {
    required DeliveryChannel channel,
    required int deliveryNumber,
    required String? eventId,
    required String check,
    required List<String> aggregates,
  }) => _store._recordFindingInTxn(
    txn,
    collector,
    role: FindingRole.restore,
    kind: FindingKind.restoreUnverified,
    evidence: <String, Object?>{
      'channel': channel.toJson(),
      'delivery_number': deliveryNumber,
      'event_id': eventId,
      'check': check,
    },
    aggregates: aggregates,
  );
}
