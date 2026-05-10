sealed class Update<T> {
  int get sequence;
  const Update();
}

class Snapshot<T> extends Update<T> {
  final T? value;
  @override
  final int sequence;
  const Snapshot({required this.value, required this.sequence});
}

class Delta<T> extends Update<T> {
  final T value;
  @override
  final int sequence;
  final String cause;
  const Delta({
    required this.value,
    required this.sequence,
    required this.cause,
  });
}

class Tombstone<T> extends Update<T> {
  final String aggregateId;
  @override
  final int sequence;
  const Tombstone({required this.aggregateId, required this.sequence});
}
