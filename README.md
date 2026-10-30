# Optional Subscripts

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml/badge.svg)](https://github.com/ollieatkinson/OptionalSubscripts/actions/workflows/tests.yml)

Read and write nested optional values, observe stores, and share asynchronous sources.

Requires Swift 6.4, macOS 12 or iOS 15. Pond and ValuePond require macOS 15 or iOS 18.

## Optional subscripts

```swift
import OptionalSubscripts

var value: Any?
value["one", 2, "three"] = 4
let number: Int = try value["one", 2, "three"]
let same: Int = try value[\.["one"][2]["three"]]
value["one", 2] = nil
```

Arrays grow with nil holes. Removing the last value prunes empty branches.

## Stores

`ValueStore` holds one Sendable value. `Dictionary.Store` provides per-key streams:

```swift
let names = [String: String].Store(["name": "Dorothy"])
var values = await names.stream("name").makeAsyncIterator()
await values.next() // .some(.some("Dorothy"))
await names.set("name", to: "Witch")
```

`Any?.Store` observes nested paths. Its payload is `SendableValue = (any Sendable)?`;
`nil` means absence and `NSNull()` represents an explicit null.

```swift
let document = Any?.Store()
await document.set("profile", "name", to: "Dorothy")
let name = try await document.get("profile", "name", as: String.self)

await document.transaction { store in
    store.set("profile", "name", to: "Witch")
    store.set("profile", "active", to: true)
}
```

Streams replay the current value. A batch emits one final snapshot per affected
route. Transactions stage writes until the outer commit; reads inside them see
committed values. Do asynchronous work before entering the transaction.

Use `SendableValue(jsonObject:)` to convert JSONSerialization output before passing
it to a store.

## Sharing streams

`ValuePond` shares a nonthrowing async sequence and replays its latest value:

```swift
let store = ValueStore(42)
let pond = ValuePond(cancellingGracePeriod: .seconds(5), source: {
    await store.stream()
})
let answer = try await pond.get()
for await value in await pond.stream() { print(value) }
```

The source stays alive while observed and for the grace period afterwards.
Expiry clears the cached value; the next subscriber starts a fresh source.

`Any?.Pond` uses a [Geyser](Sources/OptionalSubscripts/Geyser.swift) to map paths to
shared document sources. Iterate its streams with `for try await` to handle
mapping and source errors. Write through the Geyser's backing source for live
updates; `pond.store` is an inspectable cache, and edits there do not emit values.

## Routing sources

`Lake` selects a source by its deepest matching route prefix:

```swift
let local = Any?.Pond(source: Documents())
let remote = Any?.Pond(source: Documents())
typealias Routed = Any?.Pond<Documents>
let lake = try Lake<Any?.Route, Routed.Stream>(mounts: [
    .init(at: ["local"], source: { local.stream($0) }),
    .init(at: ["remote"], source: { remote.stream($0) }),
])
```

Here, `Documents` is your Geyser implementation. Open a route with
`try await lake.stream(["local", "user", "name"])`. Sources receive the full path,
including the mount prefix. An empty mount is a default; duplicate or unmounted
routes throw. A selected source's completion or failure does not try another mount.

`Trie` stores values by path and supports deepest-prefix lookup:

```swift
var trie = Trie<String, Int>(value: 0)
trie[["api", "programme"]] = 42
let value: Int? = trie[["api", "programme"]]
let fallback = trie.deepestValue(along: ["api", "programme", "title"])
```

## Observing values

Use async sequence operators before `sink`. Retain the observation while needed;
releasing or cancelling it cancels its task.

```swift
let observation = await names.stream("name").filter(String.self).sink { name in
    print(name)
}
observation.cancel()
await observation.wait()
```
