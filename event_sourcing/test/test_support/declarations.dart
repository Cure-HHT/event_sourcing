// Declarations for test fixtures. An entry type declares every event type it
// carries, and the library appends no event of an undeclared event type and
// ingests none; a fixture whose subject is not the causal record declares
// each event type it appends or ingests an eligible version.
import 'package:event_sourcing/event_sourcing.dart';

/// The declaration of [eventType] as an eligible version.
class EligibleVersion extends EventTypeDeclaration {
  const EligibleVersion(String eventType)
    : super(eventType: eventType, kind: CausalKind.version, eligible: true);
}
