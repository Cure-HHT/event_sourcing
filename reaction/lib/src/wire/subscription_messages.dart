import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';
import 'filter_codec.dart';

// --- Client -> Server messages ---

sealed class ClientMessage {
  const ClientMessage();
}

class AuthMsg extends ClientMessage {
  final String credential;
  const AuthMsg({required this.credential});
}

class SubscribeMsg extends ClientMessage {
  final String subscriptionId;
  final String viewName;
  final SubscriptionFilter? filter;
  final Set<String>? aggregates;
  const SubscribeMsg({
    required this.subscriptionId,
    required this.viewName,
    this.filter,
    this.aggregates,
  });
}

class UnsubscribeMsg extends ClientMessage {
  final String subscriptionId;
  const UnsubscribeMsg({required this.subscriptionId});
}

// --- Server -> Client messages ---

sealed class ServerMessage {
  const ServerMessage();
}

class AuthOkMsg extends ServerMessage {
  final String principalId;
  const AuthOkMsg({required this.principalId});
}

enum SubscriptionDenyReason {
  viewPermissionDenied,
  unknownView,
  malformedFilter;

  String toWire() {
    switch (this) {
      case SubscriptionDenyReason.viewPermissionDenied:
        return 'view_permission_denied';
      case SubscriptionDenyReason.unknownView:
        return 'unknown_view';
      case SubscriptionDenyReason.malformedFilter:
        return 'malformed_filter';
    }
  }

  static SubscriptionDenyReason fromWire(String s) {
    switch (s) {
      case 'view_permission_denied':
        return SubscriptionDenyReason.viewPermissionDenied;
      case 'unknown_view':
        return SubscriptionDenyReason.unknownView;
      case 'malformed_filter':
        return SubscriptionDenyReason.malformedFilter;
      default:
        throw FormatException('unknown SubscriptionDenyReason: $s');
    }
  }
}

class SubscriptionDeniedMsg extends ServerMessage {
  final String subscriptionId;
  final SubscriptionDenyReason reason;
  const SubscriptionDeniedMsg({
    required this.subscriptionId,
    required this.reason,
  });
}

enum WireErrorCode {
  internalError,
  protocolError;

  String toWire() {
    switch (this) {
      case WireErrorCode.internalError:
        return 'internal_error';
      case WireErrorCode.protocolError:
        return 'protocol_error';
    }
  }

  static WireErrorCode fromWire(String s) {
    switch (s) {
      case 'internal_error':
        return WireErrorCode.internalError;
      case 'protocol_error':
        return WireErrorCode.protocolError;
      default:
        throw FormatException('unknown WireErrorCode: $s');
    }
  }
}

class ErrorMsg extends ServerMessage {
  final WireErrorCode code;
  final String message;
  const ErrorMsg({required this.code, required this.message});
}

enum StaleDataReason {
  permissionAdded,
  roleAssigned,
  containmentChanged;

  String toWire() {
    switch (this) {
      case StaleDataReason.permissionAdded:
        return 'permission_added';
      case StaleDataReason.roleAssigned:
        return 'role_assigned';
      case StaleDataReason.containmentChanged:
        return 'containment_changed';
    }
  }

  static StaleDataReason fromWire(String s) {
    switch (s) {
      case 'permission_added':
        return StaleDataReason.permissionAdded;
      case 'role_assigned':
        return StaleDataReason.roleAssigned;
      case 'containment_changed':
        return StaleDataReason.containmentChanged;
      default:
        throw FormatException('unknown StaleDataReason: $s');
    }
  }
}

/// Server-side notification that some of the client's cached state
/// may be stale (the user's authorization changed in a UX-only
/// direction; or a containment projection moved an aggregate the
/// user holds). The server does NOT force a resubscribe — the
/// client decides.
class StaleDataMsg extends ServerMessage {
  final StaleDataReason? reason;
  const StaleDataMsg({this.reason});
}

/// Codec for the WS control-plane envelopes. Note: Update<T> envelopes
/// (server -> client) live in update_codec.dart; this codec covers
/// only the control-plane shapes.
class SubscriptionMessages {
  const SubscriptionMessages._();

  static Map<String, Object?> encodeClient(ClientMessage m) {
    if (m is AuthMsg) {
      return {'type': 'auth', 'credential': m.credential};
    } else if (m is SubscribeMsg) {
      return {
        'type': 'subscribe',
        'subscriptionId': m.subscriptionId,
        'viewName': m.viewName,
        if (m.filter != null) 'filter': FilterCodec.encode(m.filter!),
        if (m.aggregates != null) 'aggregates': m.aggregates!.toList()..sort(),
      };
    } else if (m is UnsubscribeMsg) {
      return {'type': 'unsubscribe', 'subscriptionId': m.subscriptionId};
    } else {
      throw FormatException('unknown ClientMessage: ${m.runtimeType}');
    }
  }

  static ClientMessage decodeClient(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'auth':
        return AuthMsg(credential: requireString(json, 'credential'));
      case 'subscribe':
        final aggregates = json['aggregates'];
        return SubscribeMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
          viewName: requireString(json, 'viewName'),
          filter: json['filter'] == null
              ? null
              : FilterCodec.decode(json['filter'] as Map<String, Object?>),
          aggregates: aggregates == null
              ? null
              : (aggregates as List).cast<String>().toSet(),
        );
      case 'unsubscribe':
        return UnsubscribeMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
        );
      default:
        throw FormatException('unknown client message type: $type');
    }
  }

  static Map<String, Object?> encodeServer(ServerMessage m) {
    if (m is AuthOkMsg) {
      return {'type': 'auth_ok', 'principalId': m.principalId};
    } else if (m is SubscriptionDeniedMsg) {
      return {
        'type': 'subscription_denied',
        'subscriptionId': m.subscriptionId,
        'reason': m.reason.toWire(),
      };
    } else if (m is ErrorMsg) {
      return {'type': 'error', 'code': m.code.toWire(), 'message': m.message};
    } else if (m is StaleDataMsg) {
      return {
        'type': 'stale_data',
        if (m.reason != null) 'reason': m.reason!.toWire(),
      };
    } else {
      throw FormatException('unknown ServerMessage: ${m.runtimeType}');
    }
  }

  static ServerMessage decodeServer(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'auth_ok':
        return AuthOkMsg(principalId: requireString(json, 'principalId'));
      case 'subscription_denied':
        return SubscriptionDeniedMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
          reason: SubscriptionDenyReason.fromWire(
            requireString(json, 'reason'),
          ),
        );
      case 'error':
        return ErrorMsg(
          code: WireErrorCode.fromWire(requireString(json, 'code')),
          message: requireString(json, 'message'),
        );
      case 'stale_data':
        final reason = json['reason'];
        return StaleDataMsg(
          reason: reason == null
              ? null
              : StaleDataReason.fromWire(reason as String),
        );
      default:
        throw FormatException('unknown server message type: $type');
    }
  }
}
