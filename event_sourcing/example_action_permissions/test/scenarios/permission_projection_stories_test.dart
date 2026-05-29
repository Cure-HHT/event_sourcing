// test/scenarios/permission_projection_stories_test.dart
//
// Tier-2 user-story scenarios against the action_permissions_demo server,
// driven black-box over HTTP against a real `bin/server.dart` SUBPROCESS
// (via DemoServerHarness). Each test is a multi-step narrative that
// exercises the permission system AND the projection machinery together,
// the way a real deployment would:
//
//   B9   onboarding: provisioning a user makes authorization come alive
//        from events (provision action -> user_directory + user_role_scopes
//        projections -> closed-under-events authz -> scope perimeter)
//   B10  cross-team scope perimeter holds in every direction
//   B11  required-idempotency provisioning is replay-safe and projection-
//        idempotent under retry; missing key is refused
//   B12  every dispatch outcome (allowed AND denied) is recorded in the
//        log and attributable to its initiator

import 'package:flutter_test/flutter_test.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';

import '../walkthroughs/test_support/demo_server_harness.dart';

/// Convenience matchers over the wire response union.
bool _ok(DispatchResponse r) => r is DispatchResponseSuccess;
bool _denied(DispatchResponse r) => r is DispatchResponseDenied;
String? _permDenied(DispatchResponse r) =>
    r is DispatchResponseDenied ? r.permissionDenied : null;
String? _denialKind(DispatchResponse r) =>
    r is DispatchResponseDenied ? r.denialKind : null;

// Valid per-action input payloads.
Map<String, Object?> _greenNote(String id) => <String, Object?>{
  'noteId': id,
  'title': 't',
  'body': 'b',
};
Map<String, Object?> _blueNote(String id) => <String, Object?>{
  'noteId': id,
  'title': 't',
  'body': 'b',
};
const Map<String, Object?> _press = <String, Object?>{};
const Map<String, Object?> _red = <String, Object?>{'reason': 'drill'};
const Map<String, Object?> _help = <String, Object?>{'message': 'hi'};

void main() {
  late DemoServerHarness h;

  setUp(() async {
    h = await DemoServerHarness.start();
  });
  tearDown(() => h.stop());

  // Verifies: EVS-PRD-permissions-as-events/A/B
  // Verifies: EVS-PRD-action-dispatch/A/C
  // Verifies: EVS-PRD-event-log/A/C
  test('B9: provisioning a new coordinator brings authorization alive from '
      'events, and the scope perimeter holds', () async {
    // Before provisioning, "nova" is unknown -> anonymous -> denied.
    final before = await h.dispatch(
      userId: 'nova',
      actionName: 'EditGreenNoteAction',
      rawInput: _greenNote('n1'),
    );
    expect(_denied(before), isTrue, reason: 'unknown user is denied');

    // Admin provisions nova as GreenTeam @ green-workspace.
    final prov = await h.dispatch(
      userId: 'admin-user',
      actionName: 'ProvisionUserAction',
      idempotencyKey: 'prov-nova',
      rawInput: <String, Object?>{
        'userId': 'nova',
        'role': 'GreenTeam',
        'activeSite': 'green-workspace',
      },
    );
    expect(_ok(prov), isTrue, reason: 'admin can provision');

    // The user_directory projection materialized nova from the event.
    final snap = await h.inspect();
    final nova = snap.directory.where((d) => d.userId == 'nova').toList();
    expect(nova, hasLength(1));
    expect(nova.single.role, 'GreenTeam');
    expect(nova.single.activeSite, 'green-workspace');

    // nova can now do everything GreenTeam grants...
    expect(
      _ok(
        await h.dispatch(
          userId: 'nova',
          actionName: 'EditGreenNoteAction',
          rawInput: _greenNote('n2'),
        ),
      ),
      isTrue,
    );
    expect(
      _ok(
        await h.dispatch(
          userId: 'nova',
          actionName: 'PressGreenButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );
    expect(
      _ok(
        await h.dispatch(
          userId: 'nova',
          actionName: 'PressRedAlarmAction',
          idempotencyKey: 'nova-red-1',
          rawInput: _red,
        ),
      ),
      isTrue,
    );
    expect(
      _ok(
        await h.dispatch(
          userId: 'nova',
          actionName: 'RequestHelpAction',
          rawInput: _help,
        ),
      ),
      isTrue,
    );

    // ...but NOT BlueTeam-only actions.
    final blueNote = await h.dispatch(
      userId: 'nova',
      actionName: 'EditBlueNoteAction',
      rawInput: _blueNote('b1'),
    );
    expect(_denied(blueNote), isTrue);
    expect(_permDenied(blueNote), 'notes.write.blue');
    expect(
      _denied(
        await h.dispatch(
          userId: 'nova',
          actionName: 'PressBlueButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );
  });

  // Verifies: EVS-PRD-permissions-as-events/B
  test('B10: cross-team scope perimeter holds in every direction', () async {
    // GreenTeam member: green + shared red/help allowed; blue denied.
    expect(
      _ok(
        await h.dispatch(
          userId: 'green-user-1',
          actionName: 'EditGreenNoteAction',
          rawInput: _greenNote('g1'),
        ),
      ),
      isTrue,
    );
    expect(
      _ok(
        await h.dispatch(
          userId: 'green-user-1',
          actionName: 'PressRedAlarmAction',
          idempotencyKey: 'g1-red',
          rawInput: _red,
        ),
      ),
      isTrue,
    );
    expect(
      _denied(
        await h.dispatch(
          userId: 'green-user-1',
          actionName: 'PressBlueButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );

    // BlueTeam member: the mirror image.
    expect(
      _ok(
        await h.dispatch(
          userId: 'blue-user',
          actionName: 'EditBlueNoteAction',
          rawInput: _blueNote('b1'),
        ),
      ),
      isTrue,
    );
    expect(
      _ok(
        await h.dispatch(
          userId: 'blue-user',
          actionName: 'PressRedAlarmAction',
          idempotencyKey: 'b1-red',
          rawInput: _red,
        ),
      ),
      isTrue,
    );
    expect(
      _denied(
        await h.dispatch(
          userId: 'blue-user',
          actionName: 'PressGreenButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );

    // Admin holds ONLY users.provision: team actions are denied even for
    // the admin. Least privilege, not god mode.
    expect(
      _denied(
        await h.dispatch(
          userId: 'admin-user',
          actionName: 'PressGreenButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );
    final adminRed = await h.dispatch(
      userId: 'admin-user',
      actionName: 'PressRedAlarmAction',
      idempotencyKey: 'admin-red',
      rawInput: _red,
    );
    expect(_denied(adminRed), isTrue);
    expect(_denialKind(adminRed), 'authorization_denied');
  });

  // Verifies: EVS-PRD-action-dispatch/D
  // Verifies: EVS-PRD-event-log/A/C
  test('B11: required-idempotency provisioning is replay-safe, projection-'
      'idempotent, and refuses a missing key', () async {
    // First provision succeeds.
    final first = await h.dispatch(
      userId: 'admin-user',
      actionName: 'ProvisionUserAction',
      idempotencyKey: 'prov-rex',
      rawInput: <String, Object?>{
        'userId': 'rex',
        'role': 'GreenTeam',
        'activeSite': 'green-workspace',
      },
    );
    expect(_ok(first), isTrue);

    // Replay with the same key + input is a cache hit (no re-execute).
    final replay = await h.dispatch(
      userId: 'admin-user',
      actionName: 'ProvisionUserAction',
      idempotencyKey: 'prov-rex',
      rawInput: <String, Object?>{
        'userId': 'rex',
        'role': 'GreenTeam',
        'activeSite': 'green-workspace',
      },
    );
    expect(replay, isA<DispatchResponseIdempotencyHit>());

    // The projection saw exactly ONE rex — no duplicate from the retry.
    final snap = await h.inspect();
    expect(snap.directory.where((d) => d.userId == 'rex'), hasLength(1));

    // A provision with NO idempotency key is refused (Idempotency.required).
    final noKey = await h.dispatch(
      userId: 'admin-user',
      actionName: 'ProvisionUserAction',
      rawInput: <String, Object?>{
        'userId': 'sky',
        'role': 'GreenTeam',
        'activeSite': 'green-workspace',
      },
    );
    expect(_denied(noKey), isTrue);
    expect(_denialKind(noKey), 'parse_denied');
    // ...and sky never materialized.
    final snap2 = await h.inspect();
    expect(snap2.directory.where((d) => d.userId == 'sky'), isEmpty);
  });

  // Verifies: EVS-PRD-action-dispatch/C/F
  // Verifies: EVS-PRD-event-log/A/B
  test('B12: every dispatch outcome — allowed and denied — is recorded in the '
      'log and attributed to its initiator', () async {
    // One success and one authorization denial by the same user.
    expect(
      _ok(
        await h.dispatch(
          userId: 'green-user-1',
          actionName: 'PressGreenButtonAction',
          rawInput: _press,
        ),
      ),
      isTrue,
    );
    final denied = await h.dispatch(
      userId: 'green-user-1',
      actionName: 'PressBlueButtonAction',
      rawInput: _press,
    );
    expect(_denied(denied), isTrue);

    final snap = await h.inspect();
    final mine = snap.events.where((e) => e.initiatorUserId == 'green-user-1');

    // The success produced a recorded event attributed to green-user-1
    // with the GreenTeam role.
    final successes = mine.where(
      (e) =>
          e.eventType != 'authorization_denied' &&
          e.aggregateType != 'action_attempt',
    );
    expect(
      successes,
      isNotEmpty,
      reason: 'a success event is recorded and attributed',
    );
    expect(successes.first.initiatorRole, 'GreenTeam');

    // The denial is a first-class recorded event, also attributed.
    final denials = mine.where((e) => e.eventType == 'authorization_denied');
    expect(denials, isNotEmpty, reason: 'denials are recorded in the same log');
    expect(denials.first.initiatorUserId, 'green-user-1');
  });
}
