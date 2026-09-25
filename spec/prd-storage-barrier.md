# EVS-PRD-storage-barrier: Storage barrier

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library's delivery guarantees, its views, its security-context records and the meaning of its reserved system events rest on its persisted state changing only through its own operations. This requirement makes that a run-time property of everything the library hands to application code: the library opens the storage of the backends it ships from a description the application supplies, keeps the writing access to that storage, and hands the application its public operations and reads. What lies outside the barrier -- code that holds the library's storage itself, the administrators of a Postgres database, a storage backend the application constructs, and a build with assertions enabled -- is the precondition of the storage trust boundary (EVS-PRD-destinations/L).

## Assertions

A. The library SHALL open, from a storage description the application supplies, the storage of every database it runs on a backend it ships.

B. No object the library hands to application code, and no object reachable from one through its members, SHALL carry a member that application code can invoke at run time -- statically, through a dynamic call or after a downcast -- and that writes a destination queue, a view the library materializes, a record the library keeps beside them, the security context stored beside an event, the event sequence or the storage schema version, other than the library's public operations: the event store's append, ingest, reconciliation and transaction operations, the destination registry's operations, the delivery cycle, the view rebuild, the security-context redaction, compaction, purge and retention operations, and the idempotency store's record, lookup and sweep operations.

C. No object the library hands to application code, and no object reachable from one through its members, SHALL carry a member that application code can invoke at run time and that appends an event of a reserved system entry type, appends an event without the event store's public checks, or publishes to live subscribers something the library did not commit.

D. No object the library hands to application code, and no object reachable from one through its members, SHALL yield at run time the database, connection pool, connection, session or engine transaction of a storage the library opened.

E. The library SHALL document a setup under which an application keeps its own tables in the Postgres database that holds the library's tables, through a connection it opens under a role of its own that holds no privilege to write a library table.

F. The library SHALL refuse to open a Postgres database on which a role other than the owner of its tables and the library roles the deployment declared holds a privilege to write a library table, or can act as one of those roles.

G. The library SHALL accept a storage backend instance that the application constructed only through an entry point that names the backend as application-supplied.

H. The library SHALL close the storage it opened when the event store over it closes.

I. When opening an event store fails after the library opened its storage, the library SHALL close that storage before the failure reaches the caller.

J. In a build with assertions disabled, no test seam and no test-only entry point SHALL change what the library writes, declares, records or locks, or admit a storage backend.

## Rationale

**Why a run-time barrier?** Marking the writing surface internal (EVS-PRD-destinations/K) makes a consumer's call to it an analyzer diagnostic, and nothing more: an ignore comment, an import of a `src/` file, a dynamic call or a downcast still reaches the member at run time. The library runs in regulated deployments whose invariants it enforces rather than documents, so the objects the application holds carry no writing member at all. Dart's library privacy is what enforces this: a member whose name is private to its Dart library cannot be named from another library, by the compiler or through a dynamic call, so writing access kept in such members is not handed out with the object that holds it.

**What "hands to application code" covers (assertions B to D).** Every object a public operation returns, every object the library passes to an application callback (a transaction body's transaction handle and publish collector, a boot-progress report, a subscription's emissions), and every object reachable from those through their members. An object typed as a read-only interface is a separate object that implements only the reads: a writing object typed more narrowly is recovered by a downcast. The idempotency store the library builds writes the library's idempotency table through its record and sweep operations; they are public operations like the event store's, and the table is outside the state the storage precondition protects (EVS-PRD-destinations/L), since action dispatch treats the cache as a pluggable interface. The reconciliation operation is a public operation like append: it carries the resolved state the application's user chose and runs the event store's checks (EVS-DEV-branch-conflicts/N).

**What "refused at run time" means, testably.** On both backends: a dynamic invocation, on any object the library hands out, of a member that writes, appends a reserved event, publishes, or yields a storage handle fails with `NoSuchMethodError`; a downcast of such an object to a writing type fails with a `TypeError`; a transaction handle used outside the body that received it, or with another store, is refused with `StateError` and writes nothing; and the members accessible outside their Dart library, on every type the library hands out, match a committed list, so a member added to that surface fails a test until it is reviewed. On Postgres, additionally, a transaction the application runs through the storage reader is read-only at the server, which refuses a write in it (SQLSTATE 25006); a role set up as documented for the application is refused every write to a library table by the database (SQLSTATE 42501); and a database on which a role outside the library's own may write its tables is refused at open, naming the role and the privilege.

**Layer.** Assertions B to D are structural facts about the library's own surface -- what the objects it hands out can do -- in the sense of the charter's Layer 1: they are checked, not interpreted. They are not claims about the database, which anything holding the storage's credentials or location can still write; that residual stays the storage precondition, narrowed to writers that hold the storage itself.

**Why the library opens the storage (assertion A).** A backend the application constructs over a handle it opened leaves the handle with the application: on Sembast the `Database`, on Postgres the pool and its sessions. Taking a description -- a location, or connection settings -- lets the library be the only holder of the handle it opens. The library chooses the Sembast database factory for the runtime itself, because a factory the application supplies is code that receives the opened database and can keep it.

**Why the library closes it, and on a failed open too (assertions H and I).** No one else holds the storage, so no one else can close it. An open refused after the storage was opened -- a database that must be reset, an identity mismatch, a grant refusal, a generation conflict -- would otherwise leave a pool and a lock session per attempt on Postgres, an IndexedDB connection that blocks the database's deletion in the browser, and on the native runtimes a handle that the next open of the same location in the isolate receives again.

**What the barrier does not reach.** The credentials and the location the application puts in the description: code that uses them opens its own connection or handle, and sembast returns the handle already open on a location to a second open of that location in the same isolate. The administrators of a Postgres database: the owner of the library's tables, superusers, a role holding `CREATEROLE`, and a role holding the admin option over a library role, each of which can give itself a library role's writes. The standalone Dart VM's `dart:mirrors` and its service protocol (the debugger), which can read private state. On the web, JavaScript on the page: a compiled Dart object is a JavaScript object whose private fields code holding a reference to it can walk, and any script of the origin can open the library's IndexedDB database by name. A storage backend the application constructs (assertion G). The deployment keeps these for the library; they are the storage precondition (EVS-PRD-destinations/L).

**Why builds with assertions disabled (assertion J).** The library carries test seams -- failure injection, interleaving points, and input substitutions that replace the versions a boot declares or the migrations a schema is provisioned with -- and a test-only constructor that takes a backend. They exist so tests can exercise failures and play two builds against one database. In a build with assertions disabled, which is how the library runs in production, the seams are never read and the test-only constructor refuses, so no application code can use them to change what the library writes or to hand it a backend. A build with assertions enabled (a debug build) is outside the barrier.

**Why a role of the application's own (assertions E and F).** Postgres grants cannot separate the library from the application inside one process, which share the credentials the process was given; they can separate two roles. An application that keeps tables of its own keeps them in a schema the library does not provision, under a role that holds no privilege to write the library's tables and cannot act as a library role, so every write that role makes to a library table is refused by the database. Refusing at open a database on which some other role may write the library's tables turns the deployment's grants from a documented requirement into a checked one. The library's own roles are those the deployment declared when it provisioned the database, not merely the roles one instance connects as, so instances connecting under different declared roles (a canary, a rotated credential, a separate delivery process) open side by side.

**Why name an application-supplied backend (assertion G).** The storage trust boundary admits backends the application implements. Such a backend is the trusted persistence layer of its deployment, and the application holds it by construction, so the barrier cannot cover it. An entry point that names the backend as application-supplied makes that visible where the application composes the library, instead of letting a backend instance pass where a description is expected.

## Changelog

- 2026-09-25 | 0e74a563 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-J: the library opens its shipped backends' storage from a description and closes it, on a failed open too; no object handed to application code writes persisted state, appends a reserved event, publishes, or yields a storage handle at run time; an application's own Postgres tables sit behind a role of its own, and a database on which a role outside the declared library roles may write a library table is refused at open; an application-supplied backend is accepted only through an entry point that names it; test seams and the test-only entry point have no effect with assertions disabled

*End* *Storage barrier* | **Hash**: 0e74a563
