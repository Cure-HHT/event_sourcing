// An encrypted Sembast database file on the native runtime. A file
// description with a key stores every line of the file, the compacted file
// included, as AES-256-GCM ciphertext under a fresh nonce, so no record is
// readable on disk; a key that does not decrypt the file, or no key for an
// encrypted file, is refused and the file is left byte for byte as it was;
// an unencrypted file opened with a key is converted, every record kept,
// and a crash at any point of the conversion leaves a file a later open
// converts or opens. A description without a key stores the file
// unencrypted.
@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/sembast_file_io.dart'
    show sembastConversionCopyPath, sembastConversionLockPath;
import 'package:event_sourcing/src/storage/storage_encryption.dart'
    show SembastConversionPoint, aesGcmEncrypt, encryptingSembastCodec;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;

import '../test_support/declarations.dart';
import '../test_support/tool_subprocess.dart';

const Source _source = Source(
  hopId: 'encryption-test',
  identifier: '00000000-0000-4000-8000-0000000e2c01',
  softwareVersion: 'encryption-test@1',
);

const _noteType = EntryTypeDefinition(
  id: 'encrypted_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Encrypted note',
  declarations: <EventTypeDeclaration>[EligibleVersion('written')],
);

/// Text every appended note carries, so a scan of the file finds it if any
/// record is stored readable.
const _marker = 'telangiectasia-marker';

AggregateProjectionSpec _notesView() => AggregateProjectionSpec(
  viewName: 'encrypted_notes',
  interest: SubscriptionFilter(entryTypes: <String>{_noteType.id}),
  tombstoneEventTypes: const <String>{},
);

Future<EventStore> _open(SembastStorage storage) => EventStore.open(
  storage: storage,
  entryTypes: EntryTypeRegistry()..register(_noteType),
  projections: ProjectionRegistry()..register(_notesView()),
  source: _source,
);

Future<void> _appendNotes(EventStore store, int count) async {
  for (var i = 1; i <= count; i++) {
    await store.append(
      entryType: _noteType.id,
      aggregateId: 'note-$i',
      aggregateType: 'EncryptedNote',
      eventType: 'written',
      data: <String, Object?>{'text': '$_marker $i'},
      initiator: const UserInitiator('encryption-user'),
      security: const SecurityDetails(ipAddress: '10.0.0.1'),
    );
  }
}

/// Every event of the log, as comparable values.
Future<List<Map<String, Object?>>> _events(EventStore store) async => [
  for (final event in await store.reader.findAllEvents())
    <String, Object?>{
      'sequence_number': event.sequenceNumber,
      'event_id': event.eventId,
      'event_hash': event.eventHash,
      'data': event.data,
    },
];

/// The application events of the log (the library-version events of later
/// opens excluded), as comparable values.
Future<List<Map<String, Object?>>> _notes(EventStore store) async => [
  for (final event in await _events(store))
    if ((event['data']! as Map)['text'] != null) event,
];

late Directory _dir;
var _counter = 0;

String _path() => p.join(_dir.path, 'diary-${_counter++}.db');

String _toHex(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

/// Writes a database at [path] with [count] notes, encrypted under [key]
/// when one is given, and returns its application events.
Future<List<Map<String, Object?>>> _seed(
  String path, {
  StorageEncryptionKey? key,
  int count = 5,
}) async {
  final store = await _open(SembastStorage.file(path, encryptionKey: key));
  await _appendNotes(store, count);
  final notes = await _notes(store);
  await store.close();
  return notes;
}

void main() {
  setUp(() async {
    _dir = await Directory.systemTemp.createTemp('evs-encryption-');
  });
  tearDown(() async {
    await _dir.delete(recursive: true);
  });

  group('the encryption key', () {
    // Verifies: EVS-DEV-sembast-encryption/A+B
    test('is exactly 32 bytes; any other length is refused', () {
      expect(StorageEncryptionKey(Uint8List(32)).bytes, hasLength(32));
      expect(
        () => StorageEncryptionKey(Uint8List(31)),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => StorageEncryptionKey(Uint8List(33)),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => StorageEncryptionKey(Uint8List(16)),
        throwsA(isA<ArgumentError>()),
      );
    });

    // Verifies: EVS-DEV-sembast-encryption/A
    test('copies the bytes it is given and hands out copies', () {
      final given = Uint8List(32)..[0] = 7;
      final key = StorageEncryptionKey(given);
      given[0] = 9;
      expect(key.bytes[0], 7);
      key.bytes[0] = 11;
      expect(key.bytes[0], 7);
    });

    // Verifies: EVS-DEV-sembast-encryption/D
    test('a generated key is 32 bytes and differs from the next', () {
      final a = StorageEncryptionKey.generate();
      final b = StorageEncryptionKey.generate();
      expect(a.bytes, hasLength(32));
      expect(a == b, isFalse);
    });

    // Verifies: EVS-DEV-sembast-encryption/L
    test('neither the key nor a description carrying it shows its bytes '
        'as text', () {
      final key = StorageEncryptionKey.generate();
      final hex = _toHex(key.bytes);
      final b64 = base64.encode(key.bytes);
      final storage = SembastStorage.file('x.db', encryptionKey: key);
      for (final text in <String>['$key', '$storage']) {
        expect(text, isNot(contains(hex)));
        expect(text, isNot(contains(b64)));
        expect(text, isNot(contains('${key.bytes}')));
      }
      expect('$storage', 'SembastStorage.file(x.db, encrypted)');
    });

    test('descriptions are equal only with an equal key', () {
      final key = StorageEncryptionKey.generate();
      expect(
        SembastStorage.file('x.db', encryptionKey: key),
        SembastStorage.file(
          'x.db',
          encryptionKey: StorageEncryptionKey(key.bytes),
        ),
      );
      expect(
        SembastStorage.file('x.db', encryptionKey: key) ==
            const SembastStorage.file('x.db'),
        isFalse,
      );
    });
  });

  group('the cipher', () {
    // Verifies: EVS-DEV-sembast-encryption/C
    test('is AES-256-GCM with a 128-bit tag (known answers)', () {
      // The GCM specification's test cases 13 and 14: a 256-bit zero key
      // and a 96-bit zero nonce, over no plaintext and over one zero block.
      final key = Uint8List(32);
      final nonce = Uint8List(12);
      expect(
        _toHex(aesGcmEncrypt(key, nonce, Uint8List(0))),
        '530f8afbc74536b9a963b4f1c4cb738b',
      );
      expect(
        _toHex(aesGcmEncrypt(key, nonce, Uint8List(16))),
        'cea7403d4d606b6e074ec5d3baf39d18'
        'd0d1c8a799996bf0265b98b5d48ab919',
      );
    });

    // Verifies: EVS-DEV-sembast-encryption/D
    test('encrypts the same line under a fresh nonce each time', () {
      final codec = encryptingSembastCodec(
        StorageEncryptionKey.generate(),
      ).codec!;
      const value = <String, Object?>{'text': _marker};
      final first = codec.encode(value);
      final second = codec.encode(value);
      expect(first, isNot(second));
      expect(
        base64.decode(first).sublist(0, 12),
        isNot(base64.decode(second).sublist(0, 12)),
      );
      expect(codec.decode(first), value);
      expect(codec.decode(second), value);
    });

    // Verifies: EVS-DEV-sembast-encryption/C
    test('an altered line fails to decode rather than reading back '
        'changed', () {
      final codec = encryptingSembastCodec(
        StorageEncryptionKey.generate(),
      ).codec!;
      final bytes = base64.decode(codec.encode(const {'text': _marker}));
      bytes[bytes.length - 20] ^= 1;
      expect(
        () => codec.decode(base64.encode(bytes)),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('a database opened with a key', () {
    // Verifies: EVS-PRD-storage-barrier/K, EVS-DEV-sembast-encryption/C
    test('holds no readable record content on disk, before and after a '
        'compaction', () async {
      final path = _path();
      final key = StorageEncryptionKey.generate();
      final notes = await _seed(path, key: key, count: 8);
      expect(notes, hasLength(8));

      void expectUnreadable() {
        final bytes = File(path).readAsBytesSync();
        final text = latin1.decode(bytes);
        expect(text, isNot(contains(_marker)));
        expect(text, isNot(contains('event_id')));
        expect(text, isNot(contains('"store"')));
        expect(text, isNot(contains(_noteType.id)));
        final lines = const LineSplitter().convert(text);
        final meta = json.decode(lines.first) as Map<String, Object?>;
        expect(meta['codec'], isA<String>());
        for (final line in lines.skip(1)) {
          expect(() => json.decode(line), throwsFormatException);
        }
      }

      expectUnreadable();
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.encrypted,
      );

      // A compaction writes the whole database to the file sembast then
      // moves into place, through the same codec.
      final db = await databaseFactoryIo.openDatabase(
        path,
        codec: encryptingSembastCodec(key),
      );
      await db.compact();
      await db.close();
      expectUnreadable();

      final reopened = await _open(
        SembastStorage.file(path, encryptionKey: key),
      );
      addTearDown(reopened.close);
      expect(await _notes(reopened), notes);
    });

    // Verifies: EVS-DEV-sembast-encryption/C
    test('whose last line a crash cut short still opens with every complete '
        'record', () async {
      final path = _path();
      final key = StorageEncryptionKey.generate();
      final notes = await _seed(path, key: key, count: 4);
      final file = File(path);
      final lines = const LineSplitter().convert(file.readAsStringSync());
      final last = lines.last;
      file.writeAsStringSync(
        '${lines.join('\n')}\n${last.substring(0, last.length ~/ 2)}',
      );

      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
    });
  });

  group('a key that does not decrypt the database', () {
    // Verifies: EVS-PRD-storage-barrier/L, EVS-DEV-sembast-encryption/E
    test('is refused, and the file is byte-identical afterwards', () async {
      final path = _path();
      final key = StorageEncryptionKey.generate();
      final notes = await _seed(path, key: key);
      final before = File(path).readAsBytesSync();

      await expectLater(
        _open(
          SembastStorage.file(
            path,
            encryptionKey: StorageEncryptionKey.generate(),
          ),
        ),
        throwsA(
          isA<StorageKeyMismatchError>().having(
            (e) => e.keyGiven,
            'keyGiven',
            isTrue,
          ),
        ),
      );
      expect(File(path).readAsBytesSync(), before);
      expect(File(sembastConversionCopyPath(path)).existsSync(), isFalse);

      // The right key opens it afterwards, in the same isolate.
      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
    });

    // Verifies: EVS-PRD-storage-barrier/L, EVS-DEV-sembast-encryption/E
    test('no key for an encrypted database is refused, and the file is '
        'byte-identical afterwards', () async {
      final path = _path();
      final key = StorageEncryptionKey.generate();
      final notes = await _seed(path, key: key);
      final before = File(path).readAsBytesSync();

      await expectLater(
        _open(SembastStorage.file(path)),
        throwsA(
          isA<StorageKeyMismatchError>().having(
            (e) => e.keyGiven,
            'keyGiven',
            isFalse,
          ),
        ),
      );
      expect(File(path).readAsBytesSync(), before);

      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
    });

    // Verifies: EVS-DEV-sembast-encryption/E
    test('sembast compares the codec signature before it writes: an '
        'unencrypted file opened directly with the codec is refused '
        'unchanged', () async {
      final path = _path();
      await _seed(path);
      final before = File(path).readAsBytesSync();
      await expectLater(
        databaseFactoryIo.openDatabase(
          path,
          codec: encryptingSembastCodec(StorageEncryptionKey.generate()),
        ),
        throwsA(
          isA<sembast.DatabaseException>().having(
            (e) => e.code,
            'code',
            sembast.DatabaseException.errInvalidCodec,
          ),
        ),
      );
      expect(File(path).readAsBytesSync(), before);
    });
  });

  group('a database without a key', () {
    test('is stored unencrypted, as it is read back', () async {
      final path = _path();
      final notes = await _seed(path);
      expect(File(path).readAsStringSync(), contains(_marker));
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.unencrypted,
      );
      final store = await _open(SembastStorage.file(path));
      addTearDown(store.close);
      expect(await _notes(store), notes);
    });
  });

  group('an unencrypted database opened with a key', () {
    // Verifies: EVS-PRD-storage-barrier/M, EVS-DEV-sembast-encryption/F
    test('is converted, holding every event and view row', () async {
      final path = _path();
      final plain = await _open(SembastStorage.file(path));
      await _appendNotes(plain, 6);
      final events = await _events(plain);
      final rows = (await plain.reader.findViewRows('encrypted_notes')).rows;
      final databaseId = plain.databaseId;
      await plain.close();

      final key = StorageEncryptionKey.generate();
      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(store.databaseId, databaseId);
      final after = await _events(store);
      expect(after.take(events.length).toList(), events);
      expect((await store.reader.findViewRows('encrypted_notes')).rows, rows);
      expect((await store.reader.verifyChains()).findings, isEmpty);

      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.encrypted,
      );
      expect(File(path).readAsStringSync(), isNot(contains(_marker)));
      expect(File(sembastConversionCopyPath(path)).existsSync(), isFalse);
    });

    for (final point in SembastConversionPoint.values) {
      // Verifies: EVS-PRD-storage-barrier/M, EVS-DEV-sembast-encryption/F+H+M
      test('a crash at ${point.name} leaves a file the next open opens '
          'with every event', () async {
        final path = _path();
        final notes = await _seed(path);
        final plaintext = File(path).readAsBytesSync();
        final key = StorageEncryptionKey.generate();

        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(failSembastConversion: (at) => at == point),
            () => _open(SembastStorage.file(path, encryptionKey: key)),
          ),
          throwsA(isA<InjectedFailure>()),
        );

        final copy = File(sembastConversionCopyPath(path));
        switch (point) {
          case SembastConversionPoint.copyWritten:
          case SembastConversionPoint.copyVerified:
            // The unencrypted file is untouched until the copy replaces it.
            expect(File(path).readAsBytesSync(), plaintext);
            expect(copy.existsSync(), isTrue);
            // A crash while the copy was still being written leaves it
            // short.
            final bytes = copy.readAsBytesSync();
            copy.writeAsBytesSync(bytes.sublist(0, bytes.length ~/ 2));
          case SembastConversionPoint.replaced:
            expect(
              await SembastStorage.inspectFile(path),
              SembastFileEncryption.encrypted,
            );
            expect(copy.existsSync(), isFalse);
        }

        final store = await _open(
          SembastStorage.file(path, encryptionKey: key),
        );
        addTearDown(store.close);
        expect(await _notes(store), notes);
        expect(
          await SembastStorage.inspectFile(path),
          SembastFileEncryption.encrypted,
        );
        expect(copy.existsSync(), isFalse);
        expect(File(path).readAsStringSync(), isNot(contains(_marker)));
      });
    }

    // Verifies: EVS-PRD-storage-barrier/M, EVS-DEV-sembast-encryption/F
    test('whose last line a crash cut short is converted with every complete '
        'record', () async {
      final path = _path();
      final notes = await _seed(path, count: 4);
      final file = File(path);
      final lines = const LineSplitter().convert(file.readAsStringSync());
      final last = lines.last;
      file.writeAsStringSync(
        '${lines.join('\n')}\n${last.substring(0, last.length ~/ 2)}',
      );

      final key = StorageEncryptionKey.generate();
      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.encrypted,
      );
    });

    // Verifies: EVS-DEV-sembast-encryption/I
    test('left by an interrupted sembast compaction (the file missing, the '
        'compacted file beside it) is converted with every event', () async {
      final path = _path();
      final notes = await _seed(path);
      final compacted = p.join(p.dirname(path), '~${p.basename(path)}');
      File(path).renameSync(compacted);
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.unencrypted,
      );

      final key = StorageEncryptionKey.generate();
      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
      expect(File(compacted).existsSync(), isFalse);
      expect(File(path).readAsStringSync(), isNot(contains(_marker)));
    });

    // Verifies: EVS-DEV-sembast-encryption/F
    test('with a stale compacted file beside it leaves no unencrypted copy '
        'behind', () async {
      final path = _path();
      final notes = await _seed(path);
      final compacted = p.join(p.dirname(path), '~${p.basename(path)}');
      File(path).copySync(compacted);

      final key = StorageEncryptionKey.generate();
      final store = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(store.close);
      expect(await _notes(store), notes);
      expect(File(compacted).existsSync(), isFalse);
    });
  });

  group('the conversion', () {
    // Verifies: EVS-DEV-sembast-encryption/N
    test('waits while another process holds its lock', () async {
      final path = _path();
      final notes = await _seed(path);
      final script = File(p.join(_dir.path, 'hold_lock.dart'))
        ..writeAsStringSync('''
import 'dart:io';

Future<void> main(List<String> args) async {
  final file = await File(args.single).open(mode: FileMode.append);
  await file.lock(FileLock.exclusive);
  stdout.writeln('locked');
  await stdin.first;
  await file.close();
}
''');
      final process = await Process.start(sdkTool('dart'), <String>[
        script.path,
        sembastConversionLockPath(path),
      ]);
      addTearDown(process.kill);
      final locked = await process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .firstWhere((line) => line.trim() == 'locked', orElse: () => '')
          .timeout(const Duration(minutes: 2), onTimeout: () => '');
      if (locked.isEmpty) {
        const reason = 'the lock-holding process did not start';
        if (prerequisitesRequired) fail(reason);
        markTestSkipped(reason);
        return;
      }

      final key = StorageEncryptionKey.generate();
      var opened = false;
      final opening = _open(SembastStorage.file(path, encryptionKey: key)).then(
        (store) {
          opened = true;
          return store;
        },
      );
      await Future<void>.delayed(const Duration(seconds: 2));
      expect(opened, isFalse);
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.unencrypted,
      );

      process.stdin.writeln('release');
      await process.stdin.flush();
      final store = await opening;
      addTearDown(store.close);
      expect(await _notes(store), notes);
      expect(
        await SembastStorage.inspectFile(path),
        SembastFileEncryption.encrypted,
      );
    });
  });

  group('deleting a database', () {
    // Verifies: EVS-DEV-sembast-encryption/O
    test(
      'also deletes every file an encrypted open leaves beside it',
      () async {
        final path = _path();
        await _seed(path);
        final key = StorageEncryptionKey.generate();
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failSembastConversion: (at) =>
                  at == SembastConversionPoint.copyVerified,
            ),
            () => _open(SembastStorage.file(path, encryptionKey: key)),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        final copyPath = sembastConversionCopyPath(path);
        String compactionOf(String of) =>
            p.join(p.dirname(of), '~${p.basename(of)}');
        File(path).copySync(compactionOf(path));
        File(copyPath).copySync(compactionOf(copyPath));
        final companions = <String>[
          copyPath,
          compactionOf(copyPath),
          compactionOf(path),
          sembastConversionLockPath(path),
        ];
        for (final companion in companions) {
          expect(File(companion).existsSync(), isTrue, reason: companion);
        }

        await deleteSembastDatabase(SembastStorage.file(path));

        expect(File(path).existsSync(), isFalse);
        for (final companion in companions) {
          expect(File(companion).existsSync(), isFalse, reason: companion);
        }
        expect(_dir.listSync().map((e) => p.basename(e.path)), isEmpty);
      },
    );
  });

  group('a location held open in this isolate', () {
    // Verifies: EVS-DEV-sembast-encryption/K
    test('is shared only under the same key', () async {
      final path = _path();
      final key = StorageEncryptionKey.generate();
      final first = await _open(SembastStorage.file(path, encryptionKey: key));
      addTearDown(first.close);
      await _appendNotes(first, 2);

      await expectLater(
        _open(
          SembastStorage.file(
            path,
            encryptionKey: StorageEncryptionKey.generate(),
          ),
        ),
        throwsA(isA<StorageKeyMismatchError>()),
      );
      await expectLater(
        _open(SembastStorage.file(path)),
        throwsA(isA<StorageKeyMismatchError>()),
      );

      final second = await _open(
        SembastStorage.file(
          path,
          encryptionKey: StorageEncryptionKey(key.bytes),
        ),
      );
      expect(await _notes(second), await _notes(first));
      await second.close();
      // The refusals released their holds: closing the first store closes
      // the database, so it can be deleted.
      await first.close();
      await deleteSembastDatabase(SembastStorage.file(path));
    });

    // Verifies: EVS-DEV-sembast-encryption/K
    test('held unencrypted, refuses an open with a key', () async {
      final path = _path();
      final first = await _open(SembastStorage.file(path));
      addTearDown(first.close);
      await expectLater(
        _open(
          SembastStorage.file(
            path,
            encryptionKey: StorageEncryptionKey.generate(),
          ),
        ),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('inspecting a file', () {
    // Verifies: EVS-DEV-sembast-encryption/J
    test(
      'reports a missing or empty file absent, and creates nothing',
      () async {
        final path = _path();
        expect(
          await SembastStorage.inspectFile(path),
          SembastFileEncryption.absent,
        );
        expect(File(path).existsSync(), isFalse);
        File(path).writeAsBytesSync(const <int>[]);
        expect(
          await SembastStorage.inspectFile(path),
          SembastFileEncryption.absent,
        );
      },
    );

    // Verifies: EVS-DEV-sembast-encryption/J
    test('reports unencrypted and encrypted files, writing nothing', () async {
      final plainPath = _path();
      await _seed(plainPath);
      final encryptedPath = _path();
      await _seed(encryptedPath, key: StorageEncryptionKey.generate());
      final plainBefore = File(plainPath).readAsBytesSync();
      final encryptedBefore = File(encryptedPath).readAsBytesSync();

      expect(
        await SembastStorage.inspectFile(plainPath),
        SembastFileEncryption.unencrypted,
      );
      expect(
        await SembastStorage.inspectFile(encryptedPath),
        SembastFileEncryption.encrypted,
      );
      expect(File(plainPath).readAsBytesSync(), plainBefore);
      expect(File(encryptedPath).readAsBytesSync(), encryptedBefore);
    });

    // Verifies: EVS-DEV-sembast-encryption/J
    test('refuses a file that is not a Sembast database', () async {
      final path = _path();
      File(path).writeAsStringSync('not a database\n');
      await expectLater(
        SembastStorage.inspectFile(path),
        throwsFormatException,
      );
    });
  });
}
