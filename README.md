# Optional Subscripts

Requires Swift 6.4 / Xcode 27.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml/badge.svg)](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml)

All equality expressions in this document return `true`:
```swift
import OptionalSubscripts

var 📦: Any?

📦 = nil
📦 = []
📦 = [:]

📦[] = "👋"
try 📦[] == "👋"

📦["one"] = 1
try 📦["one"] == 1

📦["one", 2] = 2
try 📦["one", 2] == 2

📦["one", 10] = nil
try 📦["one"] as [Int?] == [nil, nil, 2]

📦["one", 2] = ["three": 4]
try 📦["one", 2, "three"] == 4
try 📦[\.["one"][2]["three"]] == 4
        
📦["one", 2] = nil
📦["one"] == nil

📦["one", "two"] = nil
📦[] == nil

```

The package includes `Any?.Store` actor with routed streams, publishers, batch updates and atomic transactions:
```swift
let o = Any?.Store()

let stream = await o.stream("me", 2, "you").filter(String.self).prefix(3)

Task {
    var hearts: [String] = []
    
    for await o in stream {
        hearts.append(o)
    }
    
    hearts == ["❤️", "💛", "💚"]
}

await o.set("me", 2, "you", to: "❤️")
await o.set("me", 2, to: ["you": "💛"])
await o.set("me", to: [nil, nil, ["you": "💚"]])

```

The package also includes `Any?.Pond` actor that turns a chunky data source (like a document-oriented database) into routed data streams of arbitrary granularity:

```swift 
let db = SomeDatabase() // conforms to Geyser protocol and streams entire documents
let pond = Any?.Pond(source: db)

await db.store.set("v/2.0/way/to", "my", "heart", to: "🤍")

var hearts = ""

loop:
for await heart in pond.stream("way", "to", "my", "heart").filter(String?.self) {  
    // observing a nested field in a versioned document ↑
    
    hearts += heart ?? ""
    
    switch heart {
    case nil   where hearts.isEmpty:
               await db.store.set("v/1.0/way/to", "my", "heart", to: "❤️")
    case "❤️": await db.store.set("v/1.0/way/to", "my", "heart", to: "💛")
    case "💛": await db.store.set("v/1.0/way/to", "my", "heart", to: "💚")
    case "💚": await db.setVersion(to: "v/2.0/")
    case "🤍": break loop
    default:   assertionFailure()
    }
}

hearts == "❤️💛💚🤍"

await pond.gushSources["v/1.0/way/to"]?.referenceCount == nil
await pond.gushSources["v/2.0/way/to"]?.referenceCount == 1

```

... where:
```swift
protocol Geyser {

    associatedtype GushID: Hashable
    
    associatedtype Gushes: AsyncSequence where Gushes.Element == Any?
    
    associatedtype GushToRouteMapping: AsyncSequence where GushToRouteMapping.Element == (id: GushID, route: Optional<Any>.Route)?

    func stream(_ gush: GushID) async -> Gushes
    
    func source<Route>(of route: Route) async -> GushToRouteMapping where Route: Collection, Route.Index == Int, Route.Element == Optional<Any>.Location
}
```

### Sharing a typed source with ValuePond

`ValuePond` shares one nonthrowing `AsyncSequence`, replaying its latest
value to each subscriber. It is available on macOS 15, iOS 18, tvOS 18 and watchOS 11
or newer; the rest of the package keeps its existing deployment requirements.

```swift
let store = ValueStore(42)
let pond = ValuePond(cancellingGracePeriod: .seconds(5), source: {
    await store.stream()
})

let answer = try await pond.get() // 42
for await value in await pond.stream() {
    print(value) // replay, followed by live updates
}
```

Pending subscribers share one source. A finite source's final value stays live
while subscribed, and expiry after the last subscriber clears replay before the
next source starts. Each subscriber chooses its own buffering policy. `get()`
throws on cancellation or completion without any value; an emitted optional `nil`
is a value and is replayed normally. Carry recoverable failures as `Result` or
snapshot elements. `cancel()` closes subscribers, clears replay and lets the next
subscription start afresh. A custom `Clock<Duration>` can control grace periods.

The implementation and tests preserve the updated
[original ValuePond examples](https://gist.github.com/ollieatkinson/9f2748231612483e0cfd5c2cf76d44e9).
See [behaviour compatibility](Documentation/ValuePond.md) for the scenario mapping
and the explicit empty-source, cancellation, buffering and generation fixes.
The two types have different purposes and coexist. `ValuePond` owns one shared
value source; `Any?.Pond` maps requested routes to coarser Geyser sources and
projects their updates into fine-grained streams. Pond remains supported and its
stale-source reference counting is fixed. Its untyped `Any?` concurrency boundary
is retained in the compatibility target.

### Composing sources with Lake

`Lake<Request, Values>` routes requests to asynchronous sources. Its prefix mounts
choose the deepest match, independently of registration order. Mounts are stored
in an immutable trie built once at initialization; each lookup is O(route depth),
independent of the number of sibling mounts:

```swift
let local = [String: String].Store(["profile/name": "Dorothy"])
let remote = [String: String].Store(["remote/profile/name": "Witch"])

typealias Documents = Lake<[String], AsyncStream<String?>>
let lake = try Documents(mounts: [
    .init(at: [], source: { route in
        await local.stream(route.joined(separator: "/"))
    }),
    .init(at: ["remote"], source: { route in
        await remote.stream(route.joined(separator: "/"))
    }),
])

for await name in try await lake.stream(["remote", "profile", "name"]) {
    print(name as Any) // Optional("Witch"), followed by live updates
}
```

An empty mount route supplies a default. Duplicate routes throw at construction;
an unmatched request throws before any source opens. Prefixes match complete
components (`["api"]` does not match `["apiary"]`). Sources receive the full route,
including its mount prefix. A selected source's missing value, empty completion
or error never falls back to another mount.

Lakes nest using the same source closure:

```swift
let outer = try Documents(mounts: [
    .init(at: ["remote"], source: { try await lake.stream($0) }),
])
```

`Any?.Route` (including key and index components) can also be used directly as
the request. A request can carry more than its route. Use `init(mounts:route:)`
to extract the routing components while preserving the original request for the selected source.
Use `init(stream:)` for custom selection, such as semantic inheritance or
ambiguity checks, and delegate to the chosen source's `stream` method.

Lake returns the original sequence directly. It creates no forwarding task, actor,
buffer, replay cache or subscription registry. Sharing, pending/error values,
backpressure, grace periods and cancellation belong to the source. Reuse the same
ValuePond instance in a source closure to preserve that ValuePond's sharing across requests;
constructing a new ValuePond per request would create independent upstream work. The
Lake itself may be released while its returned sequence is being observed.

Requests, sequences and elements must be `Sendable`, and source closures are
`@Sendable`. This typed API can compose a modern `ValuePond`'s streams without
changing their lifetime. The older `Any?.Pond` above emits non-`Sendable` `Any?`
through its legacy sequence; it is **not directly compatible** with this Lake.
Use a typed `ValuePond` for new composition. Lake uses the public `Trie` below,
backed by Swift Collections’ `OrderedDictionary`. The `OptionalSubscripts` target
and its tests use Swift 6 language
mode with complete concurrency checking. They use explicit `@concurrent` task
entry and Swift 6.4 asynchronous `defer` for deterministic test cleanup.

The untyped stores, route-based Pond and Combine bridges are retained in the
`OptionalSubscriptsLegacy` compatibility target in Swift 5 mode. Unconstrained
legacy values cannot safely satisfy Swift 6's actor-to-stream transfer checks;
this separation preserves their existing source API without weakening the typed
core. `import OptionalSubscripts` continues to expose those APIs through a
re-export. Existing Hope/XCTest coverage remains in a separate legacy test target.
`swift-clocks` is a test-only dependency for deterministic grace periods;
production code uses the standard `Clock`. CI requires the `xcode-27` runner and
checks the installed Swift 6.4 toolchain.

### A path-indexed Trie

The public `Trie<Key, Value>` follows the [original Trie API](https://gist.github.com/ollieatkinson/6c11013dca11d6570112ef08f70f5a8b),
using insertion-ordered branches from Swift Collections:

```swift
var trie = Trie<String, Int>(value: 0)
trie[["api", "programme"]] = 42
let value: Int? = trie[["api", "programme"]] // 42
let subtree: Trie<String, Int>? = trie[["api"]]
let fallback = trie.deepestValue(along: ["api", "programme", "title"]) // 42
trie[["api", "other"], inserting: 10] += 1
```

The empty path addresses the root. Value assignment changes only that node's
value, preserving descendants; subtree assignment replaces its whole branch,
and assigning a nil subtree removes it. Optional nil values remain distinct
from absent values. `depthFirstSearch()` visits parents before descendants with
sorted siblings; `sorted: false` preserves sibling insertion order. Construction
uses modifying dictionary subscripts. Lake captures a completed immutable trie
for O(path-depth) source selection. Sendable conformance is conditional on the
keys and values; ordinary local tries need no concurrency constraints.

And a bunch of other (hopefully) useful new API.

(Decent documentation and an overall statement of purpose is forthcoming 😜)
