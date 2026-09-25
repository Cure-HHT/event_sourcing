// The Sembast factories on the native runtimes: the file factory, compiled
// only where dart:io exists. The counterpart of sembast_factory_web.dart,
// selected by a conditional import in sembast_factory.dart.
import 'package:meta/meta.dart' show internal;
import 'package:sembast/sembast.dart' show DatabaseFactory;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;

/// The factory for a database file on this runtime.
@internal
DatabaseFactory platformFileFactory() => databaseFactoryIo;

/// The factory for a browser database: none outside the browser.
@internal
DatabaseFactory platformBrowserFactory() => throw UnsupportedError(
  'a browser Sembast database is opened only on the web',
);
