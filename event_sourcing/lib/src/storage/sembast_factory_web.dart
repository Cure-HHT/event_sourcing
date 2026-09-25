// The Sembast factories in the browser: the IndexedDB factory, compiled only
// where the browser's interop exists. Reached only through the conditional
// import in sembast_factory.dart, so the library's core still loads on
// every runtime (EVS-PRD-portability/A).
import 'package:meta/meta.dart' show internal;
import 'package:sembast/sembast.dart' show DatabaseFactory;
import 'package:sembast_web/sembast_web.dart' show databaseFactoryWeb;

/// The factory for a database file: none in the browser.
@internal
DatabaseFactory platformFileFactory() => throw UnsupportedError(
  'a Sembast database file is opened only on the native runtimes',
);

/// The factory for a browser database on this runtime.
@internal
DatabaseFactory platformBrowserFactory() => databaseFactoryWeb;
