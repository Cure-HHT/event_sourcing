# EVS-DEV-storage-capability: The storage capability the library keeps

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations, EVS-PRD-storage-barrier

## Purpose

How the library keeps the writing access to the storage it opens. `EventStore.open` takes a storage description rather than a backend for the backends the library ships, opens the storage, and holds the backend in state private to the library. The objects it hands the application -- the event store, a storage reader, the bootstrap bundle, the destination registry, the delivery cycle, the publish collector, transaction handles, the security-context store, and the stores and policies the library builds over its storage -- carry reads and the library's public operations only.

## Assertions

A. `EventStore.open` SHALL take, for the backends the library ships, a storage description instead of a backend: a Sembast description naming a database file path, a browser database name or an in-memory database name, with an optional codec, or a Postgres description carrying the connection, lock-session and wait settings the Postgres backend opens with.

B. For a Sembast description the library SHALL open the database with a factory it selects itself: the native file factory on the native runtimes, the browser factory on the web, and the in-memory factory for an in-memory description.

C. Every member that writes the library's persisted state, appends an event without the event store's public checks, appends a reserved system event, publishes to live subscribers, sets or fires the delivery trigger, or yields a storage backend, a database, a connection pool, a connection, a session or an engine transaction, and that belongs to a type whose instances the library hands to application code (the event store, the storage reader, the bootstrap bundle, the destination registry, the delivery cycle, the publish collector, the transaction handles, the security-context store, and the stores and policies the library builds over its storage), SHALL be inaccessible outside the Dart library that declares it.

D. The members accessible outside their declaring Dart library, on every type whose instances the library hands to application code, SHALL be exactly those, by name and signature, of a list committed with the library.

E. The object the library hands to application code for each read-only interface -- the storage reader and the security-context store -- SHALL have a run-time type that declares no member writing the library's persisted state.

F. A transaction handle the library passes to an application callback SHALL yield no engine transaction, session or database through a member accessible outside its declaring Dart library.

G. The library SHALL refuse with `StateError`, writing nothing, a transaction handle used in an operation of an event store or storage reader other than the one that issued it, or after the transaction body that received it has returned.

H. On Postgres, every transaction the application runs through the storage reader SHALL run `READ ONLY` at the database.

I. The library SHALL declare no constructor accessible outside its declaring Dart library that builds a security-context store, an idempotency store or an authorization policy writing or reading through a backend or a pool the library opened.

J. `EventStore.open` SHALL accept a storage backend instance only in a description that names the backend as application-supplied.

K. `EventStore.openForTest` SHALL refuse with `StateError`, before it touches the backend, in a build with assertions disabled.

L. The library SHALL provide an operation that deletes the Sembast database a description names, and that refuses with `StateError` while an event store of the calling isolate holds that database open.

## Rationale

**Why a description (assertion A).** A description is data -- a location, connection settings, waits -- so handing it to the library hands over nothing the application keeps a reference to. The Postgres description carries what the Postgres backend opens with (the pool's and the lock session's connection settings, the TLS mode, the lock-session timeouts and heartbeat, and the boot-lock wait). Provisioning stays a separate deployment step run as the owner (EVS-DEV-postgres-backend/G), and the library opens no pool as the owner. The Sembast description names where the database lives. A codec, when given, encodes and decodes every record the library stores (its use is encryption at rest). It is code in the persistence path and therefore part of the trusted storage input, like the backend itself.

**Why the library selects the Sembast factory (assertion B).** A factory is code: one the application supplied receives the database it opens and can keep it, which would hand back the handle the barrier withholds. The library selects the factory per runtime through a conditional import, as it selects its lock-manager wrapper, so the native file factory is compiled only where `dart:io` exists and the browser factory only where the browser's interop exists. On the web every tab of an origin opens the same browser database; each tab's library opens its own handle, and the tabs stay excluded from each other by the locks the library already takes there.

**Why library privacy, not the analyzer (assertion C).** An `@internal` member is callable at run time: the analyzer reports a call, an ignore comment silences it, a `src/` import reaches every declaration that is public within the package, and a dynamic call is not analyzed at all. A member private to its Dart library cannot be named from any other library, statically or dynamically. The library's own writers need the writing access: the delivery cycle, the destination registry, the view rebuild, view convergence, the bootstrap, the retention and redaction operations, the receiver endpoint and every emitter of a reserved event. They live in other Dart libraries of the package than the event store. They receive the access through construction the library performs from the storage it opened (for example, the event store constructs the registry and the delivery cycle it hands out), or they share a Dart library with the event store; they never receive it through a member the application can reach. Transaction handles reach their engine transaction through a lookup private to the backend's own Dart library, so code outside it holds only an opaque handle.

**Why a committed surface list (assertion D).** A check of known writer names, or of member types, misses a writing method added later under a new name that returns `Future<void>`. Pinning the whole public surface of the handed-out types makes every addition fail a test until someone reviews it and adds it to the list.

**Why separate read-only objects (assertion E).** A writing object typed as a read-only interface hands its writing members to anyone who downcasts it, or calls them dynamically. The reader and the security-context store handed to the application are therefore objects of their own that delegate reads. The reader's view reads return each view's convergence state with its rows (EVS-DEV-converging-view-reads).

**Why bind a transaction handle to its issuer (assertions F and G).** The application's transaction bodies receive a handle so that its reads and the event store's appends run in one transaction. A handle carried past its body, or to another store over another database, would read or write outside the transaction the caller holds. Each store therefore records the handles it issued and refuses any other, as each backend already refuses another backend's handle (EVS-DEV-postgres-backend/L).

**Why read-only at the server (assertion H).** The reader's transaction exists for consistent reads. Running it read-only makes the server refuse a write in it, whatever path reached the session, so on Postgres the barrier does not rest on the Dart privacy of the session alone. It is read-only without being deferrable, so a reader never waits for a safe snapshot. It takes no part in the generation fence, which guards writes.

**Why the library builds its stores (assertion I).** The security-context store, the Postgres idempotency store and the authorization policy's reads work through the storage the library opened; a public constructor over the library's backend or pool would hand the application a writer over it. The library builds them and hands out their read or dispatch interfaces. An application that keeps an idempotency store of its own keeps it in its own schema, over a connection it opens under its own role (EVS-DEV-postgres-backend/O). The library's idempotency store over a pool the application opened remains available for that, since that pool is the application's.

**Why name the application-supplied path (assertion J).** An application may implement a backend (the storage trust boundary admits that), and it then holds the backend. The description that carries the backend says so, so the composition code shows which databases the barrier covers.

**Why the test-only constructor refuses without assertions (assertion K).** `openForTest` takes a raw backend and appends no library-version event (EVS-DEV-event-store-open/A). The analyzer reports a production call to it, but only at analysis time. Refusing it at run time in a build with assertions disabled uses the same gate as the library's test seams (EVS-DEV-destination-drain-lock/F), which are never read in such a build. In production, then, neither can admit a backend or change what the library records.

**Why a delete operation (assertion L).** A database written by an earlier data format is refused as one that must be reset (EVS-DEV-event-store-open/F). With the library holding the handle, the application has no sanctioned way to remove the database at the location it named. The library deletes it by location once no event store of the isolate holds it open; a failed open has already closed its storage (EVS-PRD-storage-barrier/I). Resetting a Postgres database is a deployment step taken as the owner, as provisioning is.

## Changelog

- 2026-09-25 | 445e1a1d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-L: EventStore.open takes a storage description for the shipped backends and the library selects the Sembast factory; writing, publishing and handle-yielding members of handed-out types are library-private and their public surface is a committed list; read-only interfaces are separate objects; transaction handles are opaque and bound to their issuer; Postgres reader transactions run read-only; the library builds the stores that work through its storage; a backend instance is accepted only as application-supplied; openForTest refuses with assertions disabled; the library deletes a Sembast database by its description

*End* *The storage capability the library keeps* | **Hash**: 445e1a1d
