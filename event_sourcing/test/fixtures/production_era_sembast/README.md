# An unencrypted Sembast database of data format 3.0

`diary.db` is a Sembast database file written, unencrypted, by the library
at commit `bc4a09069fea48b313bb54ed482c0dc97fb15709` (package version 0.6.1,
data format 3.0): the build an application release already in use was
built with, before the library could encrypt a database. It holds the
library's boot records, an entry type with a registered view, a registered
destination with a start
date, and events of that entry type, one of them a removal.
`expected.json` lists every event it holds, with its hash, and the rows of
its view, as that build read them back.

`test/storage/sembast_encryption_fixture_test.dart` copies the file to a
temporary directory, opens it with an encryption key through
`EventStore.open`, and checks that the converted database holds every event
of `expected.json` unchanged and stores no readable record.

## How it was made

`generator/` is a package of its own that depends on the library at the
commit above. From `generator/`:

```sh
flutter pub get
dart run bin/generate.dart ..
```

It overwrites `diary.db` and `expected.json`. The database identity, the
event ids and the timestamps differ on each run, so the test reads its
expectations from `expected.json` rather than repeating them.
