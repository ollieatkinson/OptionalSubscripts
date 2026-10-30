# Swift 6 migration

The library has one Swift 6 target, requires Swift 6.4 / Xcode 27, and uses no
Swift 5 compatibility module, unchecked Sendable conformance or preconcurrency
import to hide crossings. The original gist supplies modern ValueStore,
Dictionary.Store and ValuePond implementations/examples; it does not modernize
routed Geyser/Pond, which required separate work here.

## Actor and stream payloads

Synchronous `Any?` subscripting remains local and unchanged. Actor-bound routed
values use `SendableValue = (any Sendable)?`; Geyser documents and Pond streams
have that element type. `nil` is absence and NSNull is explicit null. Custom
scalars must conform to Sendable. Type heterogeneous containers explicitly, or
convert JSONSerialization output synchronously with `SendableValue(jsonObject:)`.
Unsupported JSON objects throw rather than entering an actor as unchecked Any.

ValueStore now requires a Sendable Value; Dictionary.Store requires Sendable
keys and values. Its gist-backed batch type is `BatchUpdate`. Typed reads still
return ordinary Sendable values. Casting errors retain a String diagnostic of
an arbitrary local input, rather than retaining a non-Sendable object in Error.

## Transactions

Both Store transaction bodies are synchronous, actor-isolated closures. Await
the transaction from outside its actor, then call its methods without await
inside. Nested transactions and rollback still work. Suspended work must happen
before entering the transaction, so unrelated concurrent tasks cannot become
fake nested levels or have their writes rolled back by another task.

## Routed Geyser/Pond

Geyser's source lookup accepts the canonical `Any?.Route` array. Sources and both
sequences must be Sendable, IDs must be Hashable and Sendable, and payloads are
SendableValue. Source sequences may throw. Routed Pond's stream is now
`AsyncThrowingStream<SendableValue, Error>`: migrate loops to `for try await`.
Missing/invalid mappings throw; empty mapping or document streams finish instead
of hanging a pending observer. A present nil document still emits absence.

Each observation owns its mapping and coarse-source lease. Shared IDs share one
upstream; changing an observer's ID cannot cancel other observers. Changing only
the projection root for the same ID replays the existing document without a
restart. Generation checks, grace leases and independent output buffers isolate
old producers and slow/cancelled consumers.

Pond.store deliberately becomes an inspectable cache. Direct cache edits no
longer drive Pond.stream. Use the Geyser's writable backing Store for live source
edits, as the README's Documents example demonstrates. Read-only sources can use
application-level preview/overlay composition. This separates observers during
remapping instead of making them all read one globally mutable forwarding bus.
Pond-owned revision trackers reject delayed cache writes after remap/cancel and
are released with their owner; injected Stores retain no per-Pond route tables.

## Async observation

Observe with `for try await`, or compose native async operators before calling
`sink`. Its value and completion handlers are Sendable async closures. Keep the
returned AsyncObservation alive for the subscription's lifetime; cancelling or
releasing it cancels its task. `wait()` joins observation completion.

## Scenario coverage

All tests use Swift Testing and snake_case names. Timing benchmarks and the old
empty XCTest method are not recreated as runtime parity assertions.

| Original behaviour | Current coverage |
| --- | --- |
| Optional path/key-path reads, sparse arrays and pruning | OptionalTests, including 12-component key paths and sendable/null conversion |
| Location literals, key/index ordering, seeded route ordering | OptionalTests |
| Tree value deletion, subtree replacement and traversal | OptionalTests over public Trie; TrieTests cover the full supplied API |
| ValueStore initialization, updates, inout notifications and identity | StoreTests, following the updated gist tests |
| Dictionary streaming, batching, nested levels and rollback | StoreTests; concurrent transaction entry is also checked |
| Routed Store fine/upstream/downstream updates, batches and concurrent writers | StoreTests, including mixed ancestor/child writes and many concurrent observations |
| Observation callbacks and batch results | StoreTests native sink; AsyncObservationTests owner release, cancellation and source failures |
| Pond version changes, nested fields and reference counts | RoutedPondTests, including partial remaps and return to reused IDs |
| Real writable database updates shared by fine observers | RoutedPondTests Store-backed Geyser; cache-only writes are explicitly excluded |
| ValuePond replay, pending consumers, expiry, buffering, restart and cancellation | ValuePondTests; compatibility mapping is in ValuePond.md |
| Lake source selection and composition | LakeTests and RoutedPondTests mounting two routed Ponds |

Additional deterministic gate tests cover pending-factory cancellation, stale
producer generations, delayed cache writes across remaps, empty/error mappings,
source failures and per-consumer buffering. Async observation cancellation is
checked while the underlying sequence remains retained.

Validation commands:

```sh
swift build -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
swift test --parallel -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
```
