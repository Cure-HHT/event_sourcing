// Implements: EVS-PRD-destinations/K
// the delivery cycle's clock type lives apart
//   from the internal drain, so the barrel exports it without exporting the
//   queue-changing functions.

/// Clock the delivery cycle reads the current time from. Tests pass a
/// fixed-time closure; production passes `null` and picks up
/// `DateTime.now().toUtc()`.
typedef Clock = DateTime Function();
