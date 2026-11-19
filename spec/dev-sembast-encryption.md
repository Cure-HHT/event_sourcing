# EVS-DEV-sembast-encryption: Encrypted Sembast database files

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-storage-barrier

## Purpose

How the library stores a Sembast database file encrypted under the key a storage description carries, refuses a key that does not open it, converts an unencrypted file, and reports which of these a file is.

## Assertions

A. The library SHALL accept, in a Sembast file description, an encryption key of exactly 32 bytes.

B. The library SHALL refuse with `ArgumentError` an encryption key of any length other than 32 bytes.

C. Where a Sembast file description carries an encryption key, the library SHALL write every record line of the database file, and the codec signature on its metadata line, as AES-256-GCM ciphertext under that key with a 128-bit authentication tag.

D. The library SHALL encrypt each line under a 96-bit nonce drawn afresh from a cryptographically secure random source.

E. If the codec signature on a database file's metadata line does not decrypt under the description's encryption key, or the file carries a codec signature and the description carries no key, then `EventStore.open` SHALL throw `StorageKeyMismatchError` and leave the file byte-identical to its state before the open.

F. When a Sembast file description carrying an encryption key names an unencrypted database, the library SHALL replace the database file with an encrypted copy only after reading the copy back from its own file under the key and finding that it holds the records of every store of the unencrypted database.

G. The library SHALL flush the encrypted copy to the storage device before it replaces the unencrypted database file with it.

H. When a Sembast file description carrying an encryption key names an unencrypted database beside which an interrupted conversion left an encrypted copy, the library SHALL discard that copy and convert the database again.

I. When a Sembast file description carrying an encryption key names a database whose file is missing and whose compaction file is present, the library SHALL open the database the compaction file holds, encrypted under the key.

J. The library SHALL provide an operation that reports whether the Sembast database at a file path is absent, unencrypted or encrypted, reading only the first line of its file and writing nothing.

K. The library SHALL refuse an open of a Sembast location that an event store of the calling isolate holds open, unless the open carries the same encryption key as the held one or both carry none.

L. The library SHALL give an encryption key, and a storage description carrying one, a text form that contains none of the key's bytes.

M. At every point of a conversion of an unencrypted database, the database file SHALL hold either the complete unencrypted database or the complete encrypted copy.

## Rationale

**Where the key comes from.** The key is data in the description, like the location: the application keeps it where it keeps its secrets and hands it over at open. Where it is kept, and how it reaches a new device, is the application's concern (EVS-PRD-storage-barrier). A browser or in-memory description carries no key: sembast's in-memory factory serializes nothing, and the browser has no database file. A description without a key stores the file as it always has, unencrypted, so a deployment that adopts nothing keeps its behaviour.

**The cipher (assertions C and D).** sembast writes a database file as one line per record, after a metadata line, and passes every record line through the codec the database is opened with; a compaction rewrites the whole file through the same codec into a file it then moves into place, so the compacted file is encrypted too. The library's codec encodes each line as base64 of the nonce, the ciphertext of the line's JSON text, and the tag. GCM is authenticated: a line altered on disk fails to decrypt rather than reading back changed, and a line that fails is reported to sembast as a format error, which is how sembast reads a last line a crash cut short, so such a file still opens with every complete record. A fresh random 96-bit nonce per line follows NIST SP 800-38D: with random nonces a key stays within the specification's bound on encryptions per key for far more writes and compactions than a device's database performs. The metadata line itself stays readable (the format version and the encoded codec signature); it carries no record.

**The cipher library.** The library's core is pure Dart and loads on every runtime (EVS-PRD-portability/A), so the AES and GCM implementations come from `pointycastle`, the Dart port of Bouncy Castle: pure Dart, synchronous, with a constant-time tag comparison, and already resolved by applications that build on the library. A synchronous cipher keeps the codec on sembast's synchronous path, which every read and compaction already takes. A platform-backed cipher would bring native code into the core and a plugin per platform; the gain, speed on large files, matters little for the line-sized records a device database holds.

**The refusal (assertions E and K).** sembast records the codec signature, encoded by the codec, on the metadata line, and compares it with the codec it is given when it reads that line, before it writes anything: a key that does not decrypt the signature, a key for an unencrypted file, and no key for an encrypted file each fail the comparison. The library reports the first and last as `StorageKeyMismatchError` and converts the second (assertion F). Writing nothing keeps a database whose key has not reached the device yet recoverable once it does. A second open in the same isolate receives the handle already open on the location, so it is refused unless it asks for the same key; a key for a location held open unencrypted is refused too, since converting needs every event store over it closed.

**The conversion (assertions F, G, H, I and M).** The unencrypted database is opened as an open without a key opens it, so a last line a crash cut short is dropped and the file compacted without it, as sembast repairs it there; every store is exported, and the export is imported into a copy beside the file under the key. The copy is then opened again from its own file and compared with the export, so what replaces the database is what the file holds, not what the library meant to write. The copy is flushed to the device and moved over the database file by one rename, which replaces it atomically and removes the unencrypted file with it: before the rename the file holds the unencrypted database, after it the complete copy (assertion M). A crash, power loss included, before the rename leaves the unencrypted database and a copy that may be short; the next open discards the copy and converts again (assertion H). A crash after it leaves the encrypted database. Without the flush, a power loss could make the rename durable before the copy's contents. sembast's own compaction stops in a similar way: when its database file is missing and its compaction file is present, sembast opens the compaction file in its place, and the library does the same before it decides whether to convert (assertion I). An unencrypted compaction file left beside a present database file is deleted, so no unencrypted copy outlives the conversion. The blocks the unencrypted file occupied are released, not overwritten; on flash storage nothing above the file system can overwrite them.

**Inspecting a file (assertion J).** An application deciding whether to create a key, fetch one, or wait for one needs to know what is on disk before it opens anything. The operation reads the metadata line: no file, or an empty one, is absent; a line without a codec signature is unencrypted; one with a signature is encrypted. It reads the compaction file when the database file is missing, as an open would.

**The text form (assertion L).** A description is logged and shown in errors; the key in it must not be.

## Changelog

- 2026-11-19 | d27e2d2d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-11-19 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-M: a 32-byte key in the Sembast file description; AES-256-GCM over every record line and the codec signature, a fresh random nonce per line; a key that does not decrypt the file, or no key for an encrypted file, refused with the file unchanged; an unencrypted file converted through a read-back, flushed copy that replaces it by one rename, an interrupted conversion's copy discarded and an interrupted compaction completed; an inspection that reads the first line only; a held location shared only when both opens carry the same key or none; text forms that omit the key

*End* *Encrypted Sembast database files* | **Hash**: d27e2d2d
