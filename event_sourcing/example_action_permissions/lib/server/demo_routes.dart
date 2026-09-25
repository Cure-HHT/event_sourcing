// lib/server/demo_routes.dart
// IMPLEMENTS REQUIREMENTS:

import 'dart:async';
import 'dart:convert';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

class DemoRoutes {
  DemoRoutes({
    required this.components,
    required this.projection,
    this.deliveryState,
  });

  final DemoServerComponents components;
  final DemoStateProjection projection;

  /// The state of this process's delivery cycle, or null when the process
  /// runs none. The `refuse-next` fault injection acts only while it is
  /// [SyncCycleState.running].
  final SyncCycleState Function()? deliveryState;

  /// Per-process trace tracking the last dispatch's stage list. The
  /// inspector pane reads this through [lastTrace]. Concurrency model:
  /// the demo is single-process and dispatches are sequential at the
  /// shelf layer; if you ever serve in parallel, scope this per-request.
  DispatchTrace? _lastTrace;

  Handler get handler {
    final router = Router()
      ..get('/healthz', _healthz)
      ..post('/session/start', _sessionStart)
      ..post('/dispatch', _dispatch)
      ..get('/demo/delivery/status', _deliveryStatus)
      ..post('/demo/delivery/halt', _deliveryHalt)
      ..post('/demo/delivery/cancel-halt', _deliveryCancelHalt)
      ..post('/demo/delivery/recover', _deliveryRecover)
      ..post('/demo/delivery/refuse-next', _deliveryRefuseNext)
      ..get('/_demo/inspect', _inspect)
      ..post('/_demo/reset', _reset);
    return router.call;
  }

  DispatchTrace? lastTrace() => _lastTrace;

  Future<Response> _healthz(Request _) async => Response.ok('ok');

  Future<Response> _sessionStart(Request req) async {
    final body = jsonDecode(await req.readAsString()) as Map<String, Object?>;
    final ssReq = SessionStartRequest.fromJson(body);
    final principal = components.directory.resolve(ssReq.userId);
    final effective = await components.policy.effectivePermissionsFor(
      principal,
    );

    final response = SessionStartResponse(
      principalRole: _principalRole(principal),
      principalUserId: principal is UserPrincipal ? principal.userId : null,
      // UserPrincipal does not carry activeSite; the demo reads it from
      // its own directory record for the wire response.
      principalActiveSite: principal is UserPrincipal
          ? components.directory.siteFor(principal.userId)
          : null,
      // effectivePermissionsFor returns an EffectiveAuthorization that
      // carries the role's permission set AND the user's scope
      // assignments. The wire response surfaces just the permission
      // names (UI gating only needs to ask "does the user's role carry
      // permission X?"); scope-by-scope display is left to richer
      // clients consuming the substrate directly.
      snapshotPermissions:
          effective.rolePermissions.map((Permission p) => p.name).toList()
            ..sort(),
    );
    return Response.ok(jsonEncode(response.toJson()), headers: _jsonHeaders);
  }

  Future<Response> _dispatch(Request req) async {
    final body = jsonDecode(await req.readAsString()) as Map<String, Object?>;
    final dReq = DispatchRequest.fromJson(body);
    final principal = components.directory.resolve(dReq.userId);
    final ctx = ActionContext(
      principal: principal,
      security: const SecurityDetails(),
      requestStartedAt: DateTime.now(),
    );

    final result = await components.dispatcher.dispatch(
      ActionSubmission(
        actionName: dReq.actionName,
        rawInput: dReq.rawInput,
        idempotencyKey: dReq.idempotencyKey,
      ),
      ctx,
    );

    final wireResponse = _toWireResponse(result, dReq.actionName);
    _lastTrace = DispatchTrace(
      // The dispatcher does not surface its invocation_id to callers; the
      // events it persists carry it in metadata. The inspector pane is the
      // source of truth for action_invocation_id correlation.
      // TODO(demo): expose invocationId on DispatchResult upstream so
      //             traces can carry it.
      actionInvocationId: '',
      actionName: dReq.actionName,
      stages: _stagesFor(result),
    );

    return Response.ok(
      jsonEncode(wireResponse.toJson()),
      headers: _jsonHeaders,
    );
  }

  // ---------------------------------------------------------------------
  // Operator routes under /demo/delivery/. Any instance serves them,
  // whether or not its delivery cycle drains: the registry operations act
  // on the database's persisted state, and the draining process honours
  // them. Each route resolves the caller's principal the way /dispatch
  // does and asks the authorization policy for `delivery.operate`, a grant
  // recorded in the log; any other decision than Allow is a 403 and
  // nothing is written. The operator's principal is recorded as the
  // initiator of the halt, cancellation and recovery events.
  // ---------------------------------------------------------------------

  /// The operator for [userId], or null when the policy does not permit
  /// `delivery.operate` to that principal.
  Future<UserPrincipal?> _operator(String? userId) async {
    final principal = components.directory.resolve(userId);
    final decision = await components.policy.isPermitted(
      principal,
      deliveryOperatePermission,
      null,
    );
    if (decision is! Allow || principal is! UserPrincipal) return null;
    return principal;
  }

  static Response _forbidden() => Response(
    403,
    body: jsonEncode(<String, Object?>{
      'error': 'the caller is not permitted ${deliveryOperatePermission.name}',
    }),
    headers: _jsonHeaders,
  );

  /// A registry refusal (a halt already open, a pending head, an unknown
  /// destination, ...) as a 409 carrying the registry's message. The
  /// registry reports its refusals as [StateError] and [ArgumentError];
  /// any other [StateError] of the operation (a closed event store) is
  /// answered the same way.
  static Response _refused(Object e) => Response(
    409,
    body: jsonEncode(<String, Object?>{
      'error': switch (e) {
        StateError(:final message) => message,
        ArgumentError(:final message) => '$message',
        _ => '$e',
      },
    }),
    headers: _jsonHeaders,
  );

  static Response _badRequest(String message) => Response(
    400,
    body: jsonEncode(<String, Object?>{'error': message}),
    headers: _jsonHeaders,
  );

  /// Runs [operation] for the operator named in [req]'s JSON body. A body
  /// that is not a JSON object, or a field of the wrong type, is a 400 and
  /// nothing is written.
  Future<Response> _asOperator(
    Request req,
    Future<Map<String, Object?>> Function(
      UserPrincipal operator,
      Map<String, Object?> body,
    )
    operation,
  ) async {
    final Map<String, Object?> body;
    final UserPrincipal? operator;
    try {
      final decoded = jsonDecode(await req.readAsString());
      if (decoded is! Map<String, Object?>) {
        throw const FormatException('the body must be a JSON object');
      }
      body = decoded;
      operator = await _operator(_stringField(body, 'userId'));
    } on FormatException catch (e) {
      return _badRequest(e.message);
    }
    if (operator == null) return _forbidden();
    try {
      return Response.ok(
        jsonEncode(await operation(operator, body)),
        headers: _jsonHeaders,
      );
    } on FormatException catch (e) {
      return _badRequest(e.message);
    } on StateError catch (e) {
      return _refused(e);
    } on ArgumentError catch (e) {
      return _refused(e);
    }
  }

  /// The string field [name] of [body]; null when absent. Throws
  /// [FormatException] for a value of another type.
  static String? _stringField(Map<String, Object?> body, String name) {
    final value = body[name];
    if (value == null || value is String) return value as String?;
    throw FormatException('$name must be a string');
  }

  /// GET /demo/delivery/status?userId=...: the persisted delivery status
  /// (`DestinationRegistry.readDeliveryStatus`) and the rows of the default
  /// destination-wedges view. The two parts are two reads: a wedge, halt or
  /// recovery that commits between them shows in one part and not yet, or
  /// no longer, in the other. Each part is consistent in itself.
  Future<Response> _deliveryStatus(Request req) async {
    final operator = await _operator(req.url.queryParameters['userId']);
    if (operator == null) return _forbidden();
    final status = await components.destinations.readDeliveryStatus();
    final wedges = await components.eventStore.reader.findViewRows(
      defaultDestinationWedgesSpec.viewName,
    );
    return Response.ok(
      jsonEncode(<String, Object?>{
        'drainer': status.drainer?.toJson(),
        'heartbeat': status.heartbeat?.toJson(),
        'destinations': <String, Object?>{
          for (final e in status.destinations.entries)
            e.key: <String, Object?>{
              'schedule': e.value.schedule.toJson(),
              'open_halt_request': e.value.openHaltRequest?.toJson(),
              'wedge': e.value.wedge?.toJson(),
              'refill_guard': e.value.refillGuard?.toJson(),
              'unserved': e.value.unserved?.wire,
            },
        },
        'wedges': wedges,
      }),
      headers: _jsonHeaders,
    );
  }

  /// POST /demo/delivery/halt {userId, destinationId, purpose}: requests a
  /// halt (`pause` or `reconfigure`); the drainer honours it by wedging the
  /// queue head.
  Future<Response> _deliveryHalt(Request req) =>
      _asOperator(req, (operator, body) async {
        final purposeWire = _stringField(body, 'purpose') ?? 'pause';
        final HaltPurpose purpose;
        try {
          purpose = HaltPurpose.fromWire(purposeWire);
        } on FormatException {
          throw ArgumentError.value(
            purposeWire,
            'purpose',
            'must be pause or reconfigure',
          );
        }
        final requestEventId = await components.destinations.requestHalt(
          _stringField(body, 'destinationId') ?? '',
          initiator: UserInitiator(operator.userId),
          purpose: purpose,
        );
        return <String, Object?>{'halt_request_event_id': requestEventId};
      });

  /// POST /demo/delivery/cancel-halt {userId, destinationId}.
  Future<Response> _deliveryCancelHalt(Request req) =>
      _asOperator(req, (operator, body) async {
        await components.destinations.cancelHalt(
          _stringField(body, 'destinationId') ?? '',
          initiator: UserInitiator(operator.userId),
        );
        return <String, Object?>{'cancelled': true};
      });

  /// POST /demo/delivery/recover {userId, destinationId, rowId}: recovers a
  /// wedged queue head (`DestinationRegistry.tombstoneAndRefill`).
  Future<Response> _deliveryRecover(Request req) =>
      _asOperator(req, (operator, body) async {
        final result = await components.destinations.tombstoneAndRefill(
          _stringField(body, 'destinationId') ?? '',
          _stringField(body, 'rowId') ?? '',
          initiator: UserInitiator(operator.userId),
        );
        return <String, Object?>{
          'row_id': result.rowId,
          'deleted_trail_count': result.deletedTrailCount,
          'rewound_to': result.rewoundTo,
        };
      });

  /// POST /demo/delivery/refuse-next {userId}: a demo fault injection, not
  /// an operator control. It simulates a receiver refusal: the next send of
  /// this process's demo destination ([logDestinationId]) returns a
  /// permanent refusal, so the drainer wedges the head and the log records
  /// the wedge with cause `permanent_refusal`, naming no operator. Only the
  /// process whose delivery cycle drains sends, so the route answers 409
  /// and arms nothing on any other process (a standby would otherwise
  /// refuse a send long after, once it took over). An operator who wants a
  /// wedge attributed to them requests a halt.
  Future<Response> _deliveryRefuseNext(Request req) =>
      _asOperator(req, (operator, body) async {
        final state = deliveryState?.call();
        if (state != SyncCycleState.running) {
          throw StateError(
            'this process does not drain (its delivery cycle is '
            '${state?.name ?? 'not started'}); send refuse-next to the '
            'process whose delivery cycle is running',
          );
        }
        components.deliveryDestination.refuseNext();
        return <String, Object?>{'refuses_next': true};
      });

  Future<Response> _inspect(Request _) async {
    final snap = await projection.snapshot();
    return Response.ok(jsonEncode(snap.toJson()), headers: _jsonHeaders);
  }

  Future<Response> _reset(Request _) async {
    // Implemented by Walkthrough 10 (Task 37) once the harness contract
    // for cold-start is finalized. Until then, callers should restart
    // the server with --ephemeral=true to wipe state.
    return Response(
      501,
      body: jsonEncode(<String, Object?>{
        'error': 'reset endpoint not yet implemented; restart with --ephemeral',
      }),
      headers: _jsonHeaders,
    );
  }

  static const Map<String, String> _jsonHeaders = <String, String>{
    'content-type': 'application/json',
  };

  String _principalRole(Principal principal) {
    return switch (principal) {
      UserPrincipal(:final activeRole) => activeRole,
      AnonymousPrincipal() => 'Anon',
    };
  }

  /// Approximate stage list inferred from the result type. The dispatcher
  /// runs Stage 1 (lookup) -> Stage 2 (invocation_id) -> [precondition
  /// idempotency-required check] -> Stage 3 (parse) -> Stage 4 (idempotency
  /// lookup) -> Stage 5 (validate) -> Stage 6 (authorize) -> Stage 7
  /// (execute) -> Stage 8 (persist) -> Stage 9 (record idempotency) ->
  /// Stage 10 (return success). Each variant tells us how far we got.
  List<String> _stagesFor(DispatchResult<Object?> result) {
    return switch (result) {
      DispatchUnknownAction() => const <String>['lookup_failed'],
      DispatchParseDenied() => const <String>['lookup', 'parse_failed'],
      DispatchIdempotencyHit() => const <String>[
        'lookup',
        'parse',
        'idempotency_hit',
      ],
      DispatchIdempotencyMismatch() => const <String>[
        'lookup',
        'parse',
        'idempotency_mismatch',
      ],
      DispatchValidationDenied() => const <String>[
        'lookup',
        'parse',
        'idempotency_check',
        'validate_failed',
      ],
      DispatchAuthorizationDenied() => const <String>[
        'lookup',
        'parse',
        'idempotency_check',
        'validate',
        'authorize_failed',
      ],
      DispatchExecutionFailed() => const <String>[
        'lookup',
        'parse',
        'idempotency_check',
        'validate',
        'authorize',
        'execute_or_persist_failed',
      ],
      DispatchSuccess() => const <String>[
        'lookup',
        'parse',
        'idempotency_check',
        'validate',
        'authorize',
        'execute',
        'persist',
        'idempotency_record',
        'return_success',
      ],
    };
  }

  DispatchResponse _toWireResponse(
    DispatchResult<Object?> result,
    String actionName,
  ) {
    return switch (result) {
      DispatchSuccess(:final result, :final emittedEventIds) =>
        DispatchResponseSuccess(
          actionInvocationId: '',
          emittedEventIds: emittedEventIds,
          result: _resultToJson(result),
        ),
      DispatchUnknownAction(:final requestedName) => DispatchResponseDenied(
        denialKind: 'unknown_action',
        actionInvocationId: '',
        errorClass: 'UnknownActionError',
        errorMessageSanitized: 'unknown action: $requestedName',
        requestedName: requestedName,
      ),
      DispatchParseDenied(:final error) => DispatchResponseDenied(
        denialKind: 'parse_denied',
        actionInvocationId: '',
        errorClass: error.runtimeType.toString(),
        errorMessageSanitized: sanitizeErrorMessage(error),
      ),
      DispatchValidationDenied(:final error) => DispatchResponseDenied(
        denialKind: 'validation_denied',
        actionInvocationId: '',
        errorClass: error.runtimeType.toString(),
        errorMessageSanitized: sanitizeErrorMessage(error),
      ),
      DispatchAuthorizationDenied(:final permission) => DispatchResponseDenied(
        denialKind: 'authorization_denied',
        actionInvocationId: '',
        errorClass: 'AuthorizationDenied',
        errorMessageSanitized: 'permission ${permission.name} not granted',
        permissionDenied: permission.name,
      ),
      DispatchExecutionFailed(:final error) => DispatchResponseDenied(
        denialKind: 'execution_failed',
        actionInvocationId: '',
        errorClass: error.runtimeType.toString(),
        errorMessageSanitized: 'execution failed',
      ),
      DispatchIdempotencyHit(
        :final cachedResult,
        :final priorEmittedEventIds,
      ) =>
        DispatchResponseIdempotencyHit(
          actionInvocationId: '',
          priorEventIds: priorEmittedEventIds,
          priorResult: _resultToJson(cachedResult),
        ),
      DispatchIdempotencyMismatch(:final actionName, :final idempotencyKey) =>
        DispatchResponseDenied(
          denialKind: 'idempotency_mismatch',
          actionInvocationId: '',
          errorClass: 'IdempotencyMismatch',
          errorMessageSanitized:
              'idempotency key "$idempotencyKey" for action '
              '"$actionName" was reused with different content',
        ),
    };
  }

  Map<String, Object?> _resultToJson(Object? result) {
    if (result == null) return const <String, Object?>{};
    if (result is Map<String, Object?>) return result;
    try {
      // ignore: avoid_dynamic_calls
      final json = (result as dynamic).toJson() as Map<String, Object?>;
      return json;
    } on Object catch (e) {
      if (e is NoSuchMethodError) {
        return <String, Object?>{'value': result.toString()};
      }
      rethrow;
    }
  }
}
