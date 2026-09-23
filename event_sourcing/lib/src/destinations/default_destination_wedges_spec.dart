// Implements: EVS-PRD-destinations/S
// the library's default destination-wedges view TREATS a destination of a
//   database as wedged from the wedge event that names it until a recovery
//   event or a deletion event that names it, deriving its rows solely from
//   those events.
// Implements: EVS-DEV-destination-drain/M
// the library's default destination-wedges view KEYS its rows by the
//   database identity the event names together with the destination
//   identifier.
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';

/// The library's default destination-wedges view: a Table view with one row
/// per wedged destination, folded from the wedge events and the recovery and
/// deletion events that end a wedge.
///
/// This is a Layer 2 convention, the library's default interpretation of
/// which destinations are wedged; the `default_` prefix of its view name
/// says so. A wedge event (`destination_wedged`) inserts the row for its
/// destination; a recovery (`destination_wedge_recovered`) or a deletion
/// (`destination_deleted`) removes it. A remove for a destination with no
/// row changes nothing.
///
/// Rows are keyed `<database_id>|<id>`: the identity of the database whose
/// drainer appended the wedge, and the destination. A row carries the wedge
/// event's data (the destination, the item and its events, the cause and the
/// attempt fields) plus the view-row fields `aggregateId` (the key) and
/// `sequence`.
///
/// Rows whose `database_id` is the reading store's own
/// (`EventStore.databaseId`) mirror that database's queues while reserved
/// system events are appended only by the library's own operations (the
/// storage trust boundary's precondition): every path that wedges a queue
/// head or ends a wedge appends its event in the same transaction. Tell
/// local rows from peer rows by the row's `database_id` field. Rows for
/// another database come from events a peer forwarded;
/// the receiver does not verify the identity they assert, and they reflect
/// the peer's queue only when the peer forwards all three event types in
/// order. `StorageBackend.wedgedFifos` reads the local queues themselves.
///
/// `EventStore.open` registers this spec; a consumer never needs to. A
/// projection registry passed to `open` may hold this same object under the
/// view name, and no other spec.
const TableProjectionSpec defaultDestinationWedgesSpec = TableProjectionSpec(
  viewName: 'default_destination_wedges',
  interest: SubscriptionFilter(
    entryTypes: <String>{
      kDestinationWedgedEntryType,
      kDestinationWedgeRecoveredEntryType,
      kDestinationDeletedEntryType,
    },
    includeSystemEvents: true,
    eventTypes: <String>{
      kDestinationWedgedEventType,
      kDestinationWedgeRecoveredEventType,
      kDestinationDeletedEventType,
    },
    aggregateTypes: <String>{kDestinationAuditAggregateType},
  ),
  insertEventTypes: <String>{kDestinationWedgedEventType},
  removeEventTypes: <String>{
    kDestinationWedgeRecoveredEventType,
    kDestinationDeletedEventType,
  },
  rowKey: CompositeKey(<String>['data.database_id', 'data.id']),
  rowData: SelectedFields(<String>[
    'id',
    'database_id',
    'row_id',
    'event_ids',
    'first_seq',
    'last_seq',
    'sequence_in_queue',
    'cause',
    'attempt_count',
    'max_attempts',
    'last_outcome',
    'http_status',
    'wire_format',
    'transform_version',
    'halt_request_event_id',
    'halt_requested_by',
    'halt_purpose',
    'drainer_epoch',
    'configuration_fingerprint',
    'configuration',
  ]),
);
