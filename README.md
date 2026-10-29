# Optional Subscripts
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/screensailor/OptionalSubscripts/actions/workflows/tests.yml/badge.svg)](https://github.com/screensailor/OptionalSubscripts/actions/workflows/tests.yml)

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

On Swift 6, `ValuePond` shares one nonthrowing `AsyncSequence`, replaying its latest
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
[original Pond examples](https://gist.github.com/ollieatkinson/9f2748231612483e0cfd5c2cf76d44e9).
See [behaviour compatibility](Documentation/ValuePond.md) for the scenario mapping
and the explicit empty-source, cancellation, buffering and generation fixes.
The older `Any?.Pond` remains available but is deprecated; its stale-source
reference counting is fixed. Its non-`Sendable` `Any?` values are not a substitute
for the typed API's concurrency contract.

And a bunch of other (hopefully) useful new API.

(Decent documentation and an overall statement of purpose is forthcoming 😜)
