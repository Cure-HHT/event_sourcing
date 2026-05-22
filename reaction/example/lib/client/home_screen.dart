// reaction/example/lib/client/home_screen.dart
//
// Authenticated home screen. Three responsibilities:
//   1) Show the current Principal + a "Revoke my role" button.
//   2) Accept a new note title and submit it through the scope's
//      ActionSubmitter.
//   3) Render the live `notes_today` view by accumulating the
//      Snapshot/Delta/Tombstone stream into a `List<Note>`.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';

/// A single note row (deduplicated by aggregate id).
class Note {
  const Note({required this.id, required this.title});

  final String id;
  final String title;
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({required this.scope, required this.baseUrl, super.key});

  final RemoteScope scope;

  /// Server base URL for the out-of-band `/admin/revoke` endpoint
  /// (which sits outside the reaction action pipeline by design — see
  /// bootstrap.dart's comment "DEMO ONLY").
  final Uri baseUrl;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final TextEditingController _titleController = TextEditingController();
  final _NotesAccumulator _notes = _NotesAccumulator();
  late final StreamSubscription<Update<Note>> _notesSub;
  String? _flash;

  @override
  void initState() {
    super.initState();
    final stream = widget.scope.viewSource.watch<Note>(
      viewName: 'notes_today',
      mapper: (row) => Note(
        id: row['aggregateId']! as String,
        title: row['title']! as String,
      ),
    );
    _notesSub = stream.listen(
      _notes.apply,
      onError: (Object err) {
        if (mounted) setState(() => _flash = 'stream error: $err');
      },
    );
    _notes.addListener(_onNotesChanged);
  }

  void _onNotesChanged() {
    if (mounted) setState(() {});
  }

  @override
  void dispose() {
    _notes.removeListener(_onNotesChanged);
    unawaited(_notesSub.cancel());
    _notes.dispose();
    _titleController.dispose();
    super.dispose();
  }

  Future<void> _submitNote() async {
    final title = _titleController.text.trim();
    if (title.isEmpty) return;
    _titleController.clear();
    try {
      final result = await widget.scope.actionSubmitter.submit(
        ActionSubmission(
          actionName: 'submit_note',
          rawInput: <String, Object?>{'title': title},
        ),
      );
      final message = switch (result) {
        DispatchSuccess<Object?>() => 'Note submitted.',
        DispatchAuthorizationDenied<Object?>(:final permission) =>
          'Denied: $permission',
        DispatchValidationDenied<Object?>(:final error) => 'Invalid: $error',
        DispatchParseDenied<Object?>(:final error) => 'Invalid: $error',
        DispatchUnknownAction<Object?>(:final requestedName) =>
          'Unknown action: $requestedName',
        DispatchIdempotencyHit<Object?>() =>
          'Already submitted (idempotency cache hit).',
        DispatchExecutionFailed<Object?>(:final error) =>
          'Execution failed: $error',
      };
      if (mounted) setState(() => _flash = message);
    } on TransportException catch (e) {
      if (mounted) setState(() => _flash = 'transport: ${e.message}');
    }
  }

  Future<void> _revokeMyRole() async {
    final principal = widget.scope.authSession.principal;
    if (principal is! UserPrincipal) {
      if (mounted) setState(() => _flash = 'Not signed in as a user.');
      return;
    }
    // Out-of-band admin endpoint — bypasses the action pipeline so the
    // revocation isn't gated by the user's own permissions.
    final url = widget.baseUrl.replace(
      path: '/admin/revoke',
      queryParameters: <String, String>{'user': principal.userId},
    );
    final client = http.Client();
    try {
      await client.post(url);
      if (mounted) {
        setState(() => _flash = 'revoke requested for ${principal.userId}');
      }
    } catch (e) {
      if (mounted) setState(() => _flash = 'revoke failed: $e');
    } finally {
      client.close();
    }
  }

  @override
  Widget build(BuildContext context) {
    final principal = widget.scope.authSession.principal;
    final userId = principal is UserPrincipal ? principal.userId : '(none)';
    final notes = _notes.notes;

    return Padding(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          Row(
            children: <Widget>[
              Expanded(child: Text('Logged in as: $userId')),
              TextButton(
                onPressed: _revokeMyRole,
                child: const Text('Revoke my role'),
              ),
            ],
          ),
          const Divider(),
          Row(
            children: <Widget>[
              Expanded(
                child: TextField(
                  controller: _titleController,
                  decoration: const InputDecoration(
                    labelText: 'New note title',
                  ),
                  onSubmitted: (_) => _submitNote(),
                ),
              ),
              const SizedBox(width: 8),
              ElevatedButton(
                onPressed: _submitNote,
                child: const Text('Submit'),
              ),
            ],
          ),
          if (_flash != null)
            Padding(
              padding: const EdgeInsets.only(top: 8),
              child: Text(_flash!, style: const TextStyle(color: Colors.grey)),
            ),
          const SizedBox(height: 16),
          Expanded(
            child: _notes.replayComplete
                ? ListView.builder(
                    itemCount: notes.length,
                    itemBuilder: (context, i) => ListTile(
                      title: Text(notes[i].title),
                      subtitle: Text(notes[i].id),
                    ),
                  )
                : const Center(child: CircularProgressIndicator()),
          ),
        ],
      ),
    );
  }
}

/// Accumulates `Update<Note>` envelopes into a flat list. Notifies
/// listeners on every state change.
class _NotesAccumulator extends ChangeNotifier {
  final Map<String, Note> _byId = <String, Note>{};
  bool _replayComplete = false;

  bool get replayComplete => _replayComplete;
  List<Note> get notes => _byId.values.toList(growable: false);

  void apply(Update<Note> update) {
    switch (update) {
      case Snapshot<Note>(:final value):
        // value == null is unusual but possible (e.g. tombstoned row in
        // replay). Nothing to add in that case.
        if (value != null) _byId[value.id] = value;
        notifyListeners();
      case EndOfReplay<Note>():
        _replayComplete = true;
        notifyListeners();
      case Delta<Note>(:final value):
        _byId[value.id] = value;
        notifyListeners();
      case Tombstone<Note>(:final aggregateId):
        if (_byId.remove(aggregateId) != null) notifyListeners();
    }
  }
}
