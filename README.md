# Optional Subscripts

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml/badge.svg)](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml)

Requires Swift 6.4 / Xcode 27. The whole library and its tests compile in Swift 6
language mode. There is one library target and no compatibility target.

### Synchronous optional subscripts

Local `Any?` values can still be read and mutated without a schema:

```swift
import OptionalSubscripts

var value: Any?
value["one", 2, "three"] = 4
let number: Int = try value["one", 2, "three"]
let same: Int = try value[\.["one"][2]["three"]]
value["one", 2] = nil // empty branches are pruned
```

Keys and integer indices remain distinct, arrays grow with nil holes, and removing
the last value prunes empty ancestors. These synchronous helpers do not make
arbitrary `Any` values safe to transfer between actors.

### Sendable stores

`ValueStore<Value: Sendable>` shares one typed value. `Dictionary.Store` requires
Sendable keys and values and provides per-key streams and batch updates. These
follow the [updated store implementations and examples](https://gist.github.com/ollieatkinson/9f2748231612483e0cfd5c2cf76d44e9).

```swift
let names = [String: String].Store(["name": "Dorothy"])
var values = await names.stream("name").makeAsyncIterator()
await values.next() // .some(.some("Dorothy"))
await names.set("name", to: "Witch")
```

`Any?.Store` retains its routed API, but its payload is now
`SendableValue = (any Sendable)?`. `nil` is absence; `NSNull()` is a present null.
Arbitrary statically Sendable scalars are supported. Arrays and dictionaries must
also have Sendable elements; heterogeneous dictionaries can be typed as
`[String: any Sendable]`.

```swift
let document = Any?.Store()
await document.set("profile", "name", to: "Dorothy")
let name = try await document.get("profile", "name", as: String.self)

// JSONSerialization returns Any. Convert synchronously before the actor hop.
let snapshot = try SendableValue(jsonObject: ["name": "Dorothy", "avatar": NSNull()])
await document.set("profile", to: snapshot)
```

Batch changes notify each affected route once. Transactions use a synchronous
actor-isolated closure, so other tasks cannot interleave as fake nested scopes.
Do asynchronous work before entering a transaction:

```swift
await document.transaction { store in
    store.set("profile", "name", to: "Witch")
    store.set("profile", "active", to: true)
}
```

### ValuePond shares one sequence

`ValuePond` shares one Sendable, nonthrowing async sequence with replay, per-consumer
buffers, cancellation and clock-controlled grace periods:

```swift
let store = ValueStore(42)
let valuePond = ValuePond(cancellingGracePeriod: .seconds(5), source: {
    await store.stream()
})
let answer = try await valuePond.get()
for await value in await valuePond.stream() { print(value) }
```

Pending consumers share one source. A finite source's last value stays live while
observed. Grace expiry clears replay before a fresh source starts. Optional nil
is replayed as a value; an empty source ends instead of hanging `get()`.
Recoverable errors can be carried as Result/snapshot elements. See
[ValuePond behaviour compatibility](Documentation/ValuePond.md).

### Pond routes coarse Geyser documents

`Any?.Pond` serves a different purpose: a `Geyser` maps fine routes to coarse
source IDs and document roots. Several child routes share one coarse upstream,
and each route's mapping can change over time. Both APIs coexist.

```swift
protocol Geyser: Sendable {
    associatedtype GushID: Hashable & Sendable
    associatedtype Gushes: AsyncSequence & Sendable
        where Gushes.Element == SendableValue
    associatedtype GushToRouteMapping: AsyncSequence & Sendable
        where GushToRouteMapping.Element == (id: GushID, route: Any?.Route)?
    func stream(_ gush: GushID) async -> Gushes
    func source(of route: Any?.Route) async -> GushToRouteMapping
}
```

A simple editable source can use a routed Store:

```swift
actor Documents: Geyser {
    typealias Gushes = AsyncStream<SendableValue>
    typealias GushToRouteMapping = AsyncStream<(id: String, route: Any?.Route)?>
    let store = Any?.Store()

    func stream(_ id: String) async -> Gushes {
        await store.stream(.key(id))
    }
    func source(of route: Any?.Route) -> GushToRouteMapping {
        let root = Array(route.prefix(2))
        return AsyncStream { output in
            output.yield((root.joined(separator: "/"), root))
            output.finish()
        }
    }
}

let database = Documents()
let pond = Any?.Pond(source: database)
await database.store.set("local/user", "name", to: "Dorothy")
for try await name in pond.stream("local", "user", "name").filter(String.self) {
    print(name)
}
```

Routed streams now throw mapping/source failures: use `for try await`. Missing or
invalid mappings fail promptly, empty mapping/gush sequences complete, and each
consumer has its own buffer and cancellation lease. A same-ID projection change
reuses the upstream; remapping one observer never cancels another's document.

**`pond.store` is now an inspectable cache, not an observation bus.** Editing that
cache no longer emits into `pond.stream`. Live edits go through the Geyser's
backing source, as in `database.store` above. Read-only providers can be composed
with application-owned preview/overlay sources instead. Cache revisions reject
obsolete writes after remapping and cancellation; metadata belongs to the Pond,
not a longer-lived injected Store.

### Lake composes sources through a Trie

Lake can directly mount routed Ponds, ValuePond streams, typed stores or other
Lakes. It returns the selected sequence directly, without a forwarding task,
actor, cache or extra buffer:

```swift
let local = Any?.Pond(source: Documents())
let remote = Any?.Pond(source: Documents())
typealias Routed = Any?.Pond<Documents>
let lake = try Lake<Any?.Route, Routed.Stream>(mounts: [
    .init(at: ["local"], source: { local.stream($0) }),
    .init(at: ["remote"], source: { remote.stream($0) }),
])
for try await value in try await lake.stream(["local", "user", "name"]) {
    print(value as Any)
}
```

The immutable mount trie is built once. Deepest-prefix lookup is O(route depth),
independent of sibling count or registration order. An empty route is a default;
duplicate or unmounted routes throw. Prefixes match complete keys/indices. A
selected source's absence, completion or error never causes fallback.

Sources receive the full request, including mount prefixes. `init(mounts:route:)`
can extract a path from a richer Sendable request without losing entity context.
`init(stream:)` supports application-defined selection such as semantic type
policy. Reuse source instances to preserve their sharing. Cancellation is checked
before and after opening a source.

The public `Trie<Key, Value>` follows the
[original path-indexed Trie API](https://gist.github.com/ollieatkinson/6c11013dca11d6570112ef08f70f5a8b)
with insertion-ordered branches from Swift Collections. It also underpins Store
route notifications:

```swift
var trie = Trie<String, Int>(value: 0)
trie[["api", "programme"]] = 42
let value: Int? = trie[["api", "programme"]]
let subtree: Trie<String, Int>? = trie[["api"]]
let fallback = trie.deepestValue(along: ["api", "programme", "title"])
```

Value assignment preserves descendants; subtree assignment replaces/removes the
branch. Optional nil is distinct from absence. Depth-first search visits parents
before descendants, with sorted or insertion-order siblings. Sendable conformance
is conditional on keys and values.

### Observing async sequences and Combine

For general concurrent code, native async operators and a task-owned `sink` accept
Sendable async handlers. Keep the returned observation alive; cancelling or
releasing it cancels its task:

```swift
let observation = await names.stream("name").filter(String.self).sink { name in
    print(name)
}
observation.cancel()
await observation.wait()
```

Real Combine interop remains available through `.publisher()`, using Swift 6.4
MainActor-isolated `Publisher`/`Subscription` conformances. Build and use those
publishers on MainActor; subscriber delivery, demand and cancellation stay there.
Sequence failure types are preserved. Standard Combine operators, `sink` and
`eraseToAnyPublisher()` work in that actor context. Arbitrary non-Sendable
subscribers are never transferred into background tasks. Stores and source
factories remain concurrent; only this UI-facing adapter is MainActor-bound.

The base deployment floors are macOS 12 / iOS 15. ValuePond, routed Pond and the
isolated Combine adapter require macOS 15 / iOS 18 equivalents. See the
[Swift 6 migration guide](Documentation/Swift6Migration.md) for API changes and
behaviour-test mapping. CI uses `xcode-27` and checks the whole module and all tests
with complete concurrency checking and warnings as errors.
