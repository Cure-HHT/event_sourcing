// Implements: EVS-PRD-destination-key-hold/K
// the library's default destination-holds view TREATS a key of a
//   destination of a database as held from the hold event that names it,
//   as releasing from the release event that names that hold, and as no
//   longer held from the clearing event that names that hold, deriving its
//   rows solely from those events, other than those whose originating
//   database is the holding database and that it does not hold as
//   authored.
// Implements: EVS-DEV-destination-drain/M
// the library's default destination-holds view KEYS its rows by the
//   database identity, the destination identifier and the block key the
//   event names.
import 'package:event_sourcing/src/destinations/default_destination_wedges_spec.dart'
    show notReceivedFromOwnIdentity;
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';

/// The library's default destination-holds view: a Table view with one row
/// per held key of a destination, folded from the hold, release and
/// clearing events and from the deletion events that remove a
/// destination's key lists.
///
/// This is a Layer 2 convention, the library's default interpretation of
/// which keys are held; the `default_` prefix of its view name says so. A
/// hold event (`destination_held`) inserts the row for its key, replacing
/// any row of an earlier hold of the key, in state `held`; a release event
/// (`destination_hold_released`) moves the row to state `releasing`; a
/// clearing event (`destination_hold_cleared`) removes it. A release or
/// clearing changes the row only while the row's `hold_id` is the one the
/// event names, and a release or clearing for a key with no row changes
/// nothing. A deletion event (`destination_deleted`) removes the row of
/// each key list it names (`key_lists`); a deletion that names none
/// changes nothing.
///
/// Rows are keyed `<database_id>|<id>|<key>`: the identity of the database
/// whose library appended the hold, the destination and the block key. A
/// row carries `id`, `database_id`, `key`, `hold_id`, `row_id` (the queue
/// item the hold tombstoned, null for a hold a release failure opened),
/// `failed_event_id`, `cause`, `attempt_count`, `event_count` (the events
/// the hold event names), `held_at` (the hold event's client timestamp)
/// and `state` (`held` or `releasing`), plus the view-row fields
/// `aggregateId` (the key) and `sequence`.
///
/// Rows whose `database_id` is the reading store's own mirror that
/// database's key lists while reserved system events are appended only by
/// the library's own operations (the storage trust boundary's
/// precondition); `DestinationRegistry.readDeliveryStatus` reads the key
/// lists themselves. Rows for another database come from events a peer
/// forwarded; the receiver does not verify the identity they assert. A
/// list an operator recovery creates again under a hold already cleared
/// shows no row: its clearing event was appended once.
///
/// The view folds no event whose originating database is the holding
/// database and that the holding database does not hold as authored, as
/// the default destination-wedges view does not.
///
/// `EventStore.open` registers this spec; a consumer never needs to. A
/// projection registry passed to `open` may hold this same object under the
/// view name, and no other spec.
const TableProjectionSpec defaultDestinationHoldsSpec = TableProjectionSpec(
  viewName: 'default_destination_holds',
  interest: SubscriptionFilter(
    entryTypes: <String>{
      kDestinationHeldEntryType,
      kDestinationHoldReleasedEntryType,
      kDestinationHoldClearedEntryType,
      kDestinationDeletedEntryType,
    },
    includeSystemEvents: true,
    eventTypes: <String>{
      kDestinationHeldEventType,
      kDestinationHoldReleasedEventType,
      kDestinationHoldClearedEventType,
      kDestinationDeletedEventType,
    },
    aggregateTypes: <String>{kDestinationAuditAggregateType},
    predicate: notReceivedFromOwnIdentity,
  ),
  insertEventTypes: <String>{kDestinationHeldEventType},
  updateEventTypes: <String>{kDestinationHoldReleasedEventType},
  removeEventTypes: <String>{kDestinationHoldClearedEventType},
  rowKey: CompositeKey(<String>['data.database_id', 'data.id', 'data.key']),
  rowData: CombinedRowData(<RowDataExtractor>[
    SelectedFields(<String>[
      'id',
      'database_id',
      'key',
      'hold_id',
      'row_id',
      'failed_event_id',
      'cause',
      'attempt_count',
    ]),
    ListLengthField('event_count', 'event_ids'),
    EventTimestampField('held_at'),
    ConstantFields(<String, Object?>{'state': 'held'}),
  ]),
  updateData: ConstantFields(<String, Object?>{'state': 'releasing'}),
  matchField: 'hold_id',
  listedRemovals: ListedRowKeys(
    eventTypes: <String>{kDestinationDeletedEventType},
    listField: 'key_lists',
    paths: <String>['data.database_id', 'data.id', 'item.key'],
  ),
);
