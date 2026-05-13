// Implements: EVS-PRD-permissions-as-events (scope-value carrier for grants and dispatch)

/// The scope a permission grant or an action-dispatch operation targets.
///
/// Sealed: every consumer-side switch must exhaustively handle all three
/// variants. Adding a fourth variant is a deliberate code-plus-REQ change.
///
/// JSON parse contract: the three shapes are mutually exclusive by key set.
/// `fromJson` rejects any object whose keys do not match exactly one shape.
sealed class ScopeValue {
  const ScopeValue();

  factory ScopeValue.fromJson(Map<String, Object?> json) {
    final hasClass = json.containsKey('class');
    final hasValue = json.containsKey('value');
    final hasValueWildcard = json.containsKey('wildcard_value');
    final hasClassWildcard = json.containsKey('wildcard_class');

    if (hasClassWildcard && !hasClass && !hasValue && !hasValueWildcard) {
      if (json['wildcard_class'] != true) {
        throw FormatException(
          'wildcard_class must be the literal `true`, got ${json['wildcard_class']}',
        );
      }
      if (json.length != 1) {
        throw FormatException(
          'wildcard_class object has unexpected keys: $json',
        );
      }
      return const TotalWildcardScope();
    }

    if (hasClass && hasValueWildcard && !hasValue && !hasClassWildcard) {
      if (json['wildcard_value'] != true) {
        throw FormatException(
          'wildcard_value must be the literal `true`, got ${json['wildcard_value']}',
        );
      }
      if (json.length != 2) {
        throw FormatException(
          'value-wildcard object has unexpected keys: $json',
        );
      }
      final cls = json['class'];
      if (cls is! String || cls.isEmpty) {
        throw FormatException('class must be a non-empty string, got $cls');
      }
      return ValueWildcardScope(class_: cls);
    }

    if (hasClass && hasValue && !hasValueWildcard && !hasClassWildcard) {
      if (json.length != 2) {
        throw FormatException('bound object has unexpected keys: $json');
      }
      final cls = json['class'];
      final val = json['value'];
      if (cls is! String || cls.isEmpty) {
        throw FormatException('class must be a non-empty string, got $cls');
      }
      if (val is! String || val.isEmpty) {
        throw FormatException('value must be a non-empty string, got $val');
      }
      return BoundScope(class_: cls, value: val);
    }

    throw FormatException(
      'ScopeValue JSON shape unrecognized; expected one of: '
      '{"class","value"}, {"class","wildcard_value":true}, '
      '{"wildcard_class":true}. Got: $json',
    );
  }

  Map<String, Object?> toJson();
}

/// Specific value of a specific scope class.
final class BoundScope extends ScopeValue {
  const BoundScope({required this.class_, required this.value})
    : assert(class_ != '', 'class_ must not be empty'),
      assert(value != '', 'value must not be empty');

  final String class_;
  final String value;

  @override
  Map<String, Object?> toJson() => {'class': class_, 'value': value};

  @override
  bool operator ==(Object other) =>
      other is BoundScope && class_ == other.class_ && value == other.value;

  @override
  int get hashCode => Object.hash('Bound', class_, value);

  @override
  String toString() => 'BoundScope($class_, $value)';
}

/// Any value of a specific scope class.
final class ValueWildcardScope extends ScopeValue {
  const ValueWildcardScope({required this.class_})
    : assert(class_ != '', 'class_ must not be empty');

  final String class_;

  @override
  Map<String, Object?> toJson() => {'class': class_, 'wildcard_value': true};

  @override
  bool operator ==(Object other) =>
      other is ValueWildcardScope && class_ == other.class_;

  @override
  int get hashCode => Object.hash('ValueWildcard', class_);

  @override
  String toString() => 'ValueWildcardScope($class_)';
}

/// Any class, any value (the global/admin grant).
final class TotalWildcardScope extends ScopeValue {
  const TotalWildcardScope();

  @override
  Map<String, Object?> toJson() => {'wildcard_class': true};

  @override
  bool operator ==(Object other) => other is TotalWildcardScope;

  @override
  int get hashCode => Object.hash('TotalWildcard', null);

  @override
  String toString() => 'TotalWildcardScope()';
}
