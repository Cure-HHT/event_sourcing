// Implements: EVS-DEV-event-record/A
// the client-timestamp form a record may carry: a four-digit year,
//   calendar fields within their ranges, and an explicit UTC offset.
import 'package:meta/meta.dart' show internal;

/// The date-time grammar `DateTime.parse` reads, narrowed to an unsigned
/// four-digit year and a required offset (`Z`, or a signed hours offset
/// with optional minutes, directly after the time: the rule
/// `ProvenanceEntry.fromJson` applies to `received_at`).
final RegExp _recordTimestamp = RegExp(
  r'^(\d{4})-?(\d\d)-?(\d\d)'
  r'[ T](\d\d)(?::?(\d\d)(?::?(\d\d)(?:[.,]\d+)?)?)?'
  r'(?:Z|[+-](\d\d)(?::?(\d\d))?)$',
);

/// Parses [text], a record's client timestamp, to the instant it names.
///
/// Throws [FormatException] unless [text] has a four-digit year, a month,
/// day, hour, minute and second each within its calendar range (so no
/// 30 February, no hour 24 and no 60th second, which `DateTime.parse`
/// would roll over to another instant), and an explicit UTC offset of at
/// most 23 hours and 59 minutes. Every such timestamp names one instant on
/// every host, whatever its zone, and lies within the range every storage
/// backend stores.
@internal
DateTime parseRecordTimestamp(String text) {
  final match = _recordTimestamp.firstMatch(text);
  if (match == null) {
    throw FormatException(
      'expected an ISO 8601 date-time with a four-digit year and an '
      'explicit offset (Z or +/-HH[:]MM), got "$text"',
    );
  }
  int field(int group) {
    final digits = match.group(group);
    return digits == null ? 0 : int.parse(digits);
  }

  final year = field(1);
  final month = field(2);
  final day = field(3);
  void within(String name, int value, int min, int max) {
    if (value < min || value > max) {
      throw FormatException('$name $value is outside $min to $max in "$text"');
    }
  }

  within('month', month, 1, 12);
  within('day', day, 1, _daysIn(year, month));
  within('hour', field(4), 0, 23);
  within('minute', field(5), 0, 59);
  within('second', field(6), 0, 59);
  within('offset hours', field(7), 0, 23);
  within('offset minutes', field(8), 0, 59);
  return DateTime.parse(text);
}

int _daysIn(int year, int month) {
  if (month == 2) {
    final leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    return leap ? 29 : 28;
  }
  return const <int>[4, 6, 9, 11].contains(month) ? 30 : 31;
}
