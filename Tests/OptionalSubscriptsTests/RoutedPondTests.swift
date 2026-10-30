import Clocks
@testable import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1))) struct RoutedPondTests {
    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func replay_populates_a_new_cache_mount_after_the_source_finishes() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let first: Any?.Route = ["first", "name"]
        var values = pond.stream(first).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(first, to: "document", at: ["first"])
        try await routedWait { await source.starts["document"] == 1 }
        await source.send("document", value: ["name": "Cached"])
        #expect(try await values.next() as? String == "Cached")
        try await routedWait { (try? await pond.store.get("first", "name", as: String.self)) == "Cached" }
        await source.finish("document")
        let second: Any?.Route = ["second", "name"]
        var replay = pond.stream(second).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(second, to: "document", at: ["second"])
        #expect(try await replay.next() as? String == "Cached")
        try await routedWait { (try? await pond.store.get("second", "name", as: String.self)) == "Cached" }
        #expect(await source.starts["document"] == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func same_id_projection_changes_populate_the_new_cache_mount() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let route: Any?.Route = ["doc", "child", "value"]
        var values = pond.stream(route).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "document", at: ["doc"])
        try await routedWait { await source.starts["document"] == 1 }
        await source.send("document", value: ["value": "new", "child": ["value": "old"]])
        #expect(try await values.next() as? String == "old")
        try await routedWait { (try? await pond.store.get("doc", "child", "value", as: String.self)) == "old" }
        await source.map(route, to: "document", at: ["doc", "child"])
        #expect(try await values.next() as? String == "new")
        try await routedWait { (try? await pond.store.get("doc", "child", "value", as: String.self)) == "new" }
        #expect(await source.starts["document"] == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func distinct_child_routes_share_one_coarse_source_and_cancel_independently() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let red: Any?.Route = ["way", "to", "red", "heart"]
        let blue: Any?.Route = ["way", "to", "blue", "heart"]
        let redValues = ValueStore([String]())
        let blueValues = ValueStore([String]())
        let first = pond.stream(red).filter(String.self).sink { value in await redValues.inout { $0.append(value) } }
        let second = pond.stream(blue).filter(String.self).sink { value in await blueValues.inout { $0.append(value) } }
        try await routedWait { await source.mappingCount == 2 }
        await source.map(red, to: "document", at: ["way", "to"])
        await source.map(blue, to: "document", at: ["way", "to"])
        try await routedWait { await pond.gushSources["document"]?.referenceCount == 2 }
        try await routedWait { await source.starts["document"] == 1 }
        await source.send("document", value: ["red": ["heart": "red"], "blue": ["heart": "blue"]])
        try await routedWait {
            let red = await redValues.value
            let blue = await blueValues.value
            return red == ["red"] && blue == ["blue"]
        }
        first.cancel(); await first.wait()
        try await routedWait { await pond.gushSources["document"]?.referenceCount == 1 }
        await source.send("document", value: ["blue": ["heart": "white"]])
        try await routedWait { await blueValues.value == ["blue", "white"] }
        second.cancel(); await second.wait()
        try await routedWait { await pond.gushSources["document"] == nil }
        #expect(await source.starts["document"] == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func live_versions_preserve_absence_nested_updates_and_return_to_a_previous_id() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let route: Any?.Route = ["way", "to", "my", "heart"]
        var values = pond.stream(route, bufferingPolicy: .unbounded).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "v1", at: ["way", "to"])
        try await routedWait { await source.starts["v1"] == 1 }
        await source.send("v1", value: nil)
        let missing = try await values.next()
        #expect(missing != nil)
        #expect(missing! == nil)
        for heart in ["red", "yellow", "green"] {
            await source.send("v1", value: ["my": ["heart": heart]])
            #expect(try await values.next() as? String == heart)
        }
        await source.map(route, to: "v2", at: ["way", "to"])
        try await routedWait { await source.starts["v2"] == 1 }
        await source.send("v2", value: ["my": ["heart": "white"]])
        #expect(try await values.next() as? String == "white")
        #expect(await pond.gushSources["v1"] == nil)
        await source.map(route, to: "v1", at: ["way", "to"])
        try await routedWait { await source.starts["v1"] == 2 }
        await source.send("v1", value: ["my": ["heart": "red"]])
        #expect(try await values.next() as? String == "red")
        #expect(await pond.gushSources["v2"] == nil)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func remapping_one_child_preserves_other_observers_of_the_old_document() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let a: Any?.Route = ["doc", "a"]
        let b: Any?.Route = ["doc", "b"]
        var first = pond.stream(a).makeAsyncIterator()
        var second = pond.stream(b).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(a, to: "old", at: ["doc"])
        await source.map(b, to: "old", at: ["doc"])
        try await routedWait { await pond.gushSources["old"]?.referenceCount == 2 }
        try await routedWait { await source.starts["old"] == 1 }
        await source.send("old", value: ["a": "A0", "b": "B0"])
        #expect(try await first.next() as? String == "A0")
        #expect(try await second.next() as? String == "B0")
        await source.map(a, to: "new", at: ["doc"])
        try await routedWait { await source.starts["new"] == 1 }
        await source.send("new", value: ["a": "A1"])
        #expect(try await first.next() as? String == "A1")
        await source.send("old", value: ["b": "B1"])
        #expect(try await second.next() as? String == "B1")
        #expect(await source.starts["old"] == 1)
        #expect(await pond.gushSources["old"]?.referenceCount == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func same_id_projection_changes_replay_without_restarting_upstream() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let route: Any?.Route = ["document", "nested", "leaf"]
        var values = pond.stream(route, bufferingPolicy: .unbounded).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "same", at: ["document"])
        try await routedWait { await source.starts["same"] == 1 }
        let snapshot: [String: any Sendable] = ["nested": ["leaf": "deep"], "leaf": "shallow"]
        await source.send("same", value: snapshot)
        #expect(try await values.next() as? String == "deep")
        await source.map(route, to: "same", at: ["document", "nested"])
        #expect(try await values.next() as? String == "shallow")
        #expect(await source.starts["same"] == 1)
        #expect(await pond.gushSources["same"]?.referenceCount == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func missing_invalid_and_empty_mappings_complete_instead_of_hanging() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let absent: Any?.Route = ["absent"]
        var missing = pond.stream(absent).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(absent, to: nil, at: [])
        await #expect(throws: Any?.Pond<RoutedSource>.RoutingError.unmounted(absent)) { try await missing.next() }
        try await routedWait { await source.endedMappings == 1 }
        let route: Any?.Route = ["items", 1, "leaf"]
        var invalid = pond.stream(route).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(route, to: "bad", at: ["items", "1"])
        await #expect(throws: Any?.Pond<RoutedSource>.RoutingError.invalidMapping(["items", "1"])) { try await invalid.next() }
        let empty: Any?.Route = ["empty"]
        var finished = pond.stream(empty).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 3 }
        await source.finishMapping(empty)
        #expect(try await finished.next() == nil)
        let failed: Any?.Route = ["mapping-failure"]
        var failure = pond.stream(failed).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 4 }
        await source.finishMapping(failed, error: RoutedFailure.upstream)
        await #expect(throws: RoutedFailure.upstream) { try await failure.next() }
        #expect(await source.starts.isEmpty)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func remapping_before_a_first_gush_discards_the_old_pending_source() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let route: Any?.Route = ["doc", "leaf"]
        var values = pond.stream(route).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "old", at: ["doc"])
        try await routedWait { await source.starts["old"] == 1 }
        await source.map(route, to: "new", at: ["doc"])
        try await routedWait { await source.starts["new"] == 1 }
        await source.send("old", value: ["leaf": "stale"])
        await source.send("new", value: ["leaf": "current"])
        #expect(try await values.next() as? String == "current")
        #expect(await pond.gushSources["old"] == nil)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func last_observer_cancellation_reaches_a_pending_factory() async throws {
        let gate = RoutedGate()
        let source = RoutedSource(factoryGate: gate)
        let pond = Any?.Pond(source: source)
        let route: Any?.Route = ["doc", "leaf"]
        let observation = pond.stream(route).sink { _ in }
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "gated", at: ["doc"])
        var entered = gate.entered.makeAsyncIterator()
        _ = await entered.next()
        observation.cancel(); await observation.wait()
        try await routedWait { await source.cancelledFactories == 1 }
        #expect(await pond.gushSources["gated"] == nil)
        await gate.release()
        await pond.cancel()
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func empty_and_failing_gushes_complete_pending_observers() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let empty: Any?.Route = ["empty"]
        var values = pond.stream(empty).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 1 }
        await source.map(empty, to: "empty", at: [])
        try await routedWait { await source.starts["empty"] == 1 }
        await source.finish("empty")
        #expect(try await values.next() == nil)
        let failed: Any?.Route = ["failed"]
        var errors = pond.stream(failed).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(failed, to: "failed", at: [])
        try await routedWait { await source.starts["failed"] == 1 }
        await source.finish("failed", error: RoutedFailure.upstream)
        await #expect(throws: RoutedFailure.upstream) { try await errors.next() }
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func fine_observers_keep_independent_buffering_policies() async throws {
        let source = RoutedSource()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        let a: Any?.Route = ["doc", "a"]
        let b: Any?.Route = ["doc", "b"]
        var fast = pond.stream(a, bufferingPolicy: .unbounded).makeAsyncIterator()
        var slow = pond.stream(b, bufferingPolicy: .bufferingNewest(1)).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(a, to: "doc", at: ["doc"])
        await source.map(b, to: "doc", at: ["doc"])
        try await routedWait { await pond.gushSources["doc"]?.referenceCount == 2 }
        try await routedWait { await source.starts["doc"] == 1 }
        await source.send("doc", value: ["a": 1, "b": 1])
        #expect(try await fast.next() as? Int == 1)
        await source.send("doc", value: ["a": 2, "b": 2])
        #expect(try await fast.next() as? Int == 2)
        #expect(try await slow.next() as? Int == 2)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func a_cancelled_factory_cannot_interfere_with_a_reused_gush_id() async throws {
        let gate = RoutedGate()
        let source = RoutedSource(factoryGate: gate)
        let pond = Any?.Pond(source: source)
        defer { await gate.release(); await pond.cancel() }
        let route: Any?.Route = ["doc", "leaf"]
        let first = pond.stream(route).sink { _ in }
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "gated", at: ["doc"])
        var entered = gate.entered.makeAsyncIterator()
        _ = await entered.next()
        first.cancel(); await first.wait()
        try await routedWait { await pond.gushSources["gated"] == nil }
        var current = pond.stream(route).makeAsyncIterator()
        try await routedWait { await source.mappingCount == 2 }
        await source.map(route, to: "gated", at: ["doc"])
        try await routedWait { await source.starts["gated"] == 2 }
        await source.send("gated", value: ["leaf": "current"])
        #expect(try await current.next() as? String == "current")
        await gate.release()
        await source.send("gated", value: ["leaf": "next"])
        #expect(try await current.next() as? String == "next")
        #expect(await pond.gushSources["gated"]?.referenceCount == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func grace_period_retains_one_upstream_and_expires_after_the_last_observer() async throws {
        let source = RoutedSource()
        let clock = TestClock()
        let pond = Any?.Pond(source: source, cancellingGracePeriod: .seconds(5), clock: clock)
        defer { await pond.cancel() }
        let route: Any?.Route = ["doc"]
        let first = pond.stream(route).sink { _ in }
        try await routedWait { await source.mappingCount == 1 }
        await source.map(route, to: "doc", at: [])
        try await routedWait { await source.starts["doc"] == 1 }
        first.cancel(); await first.wait()
        try await routedWait { await pond.gushSources["doc"]?.referenceCount == 0 }
        await clock.advance(by: .seconds(3))
        #expect(await pond.gushSources["doc"] != nil)
        await clock.advance(by: .seconds(2))
        try await routedWait { await pond.gushSources["doc"] == nil }
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func backing_store_edits_drive_shared_pond_observers_but_cache_edits_do_not() async throws {
        let source = StoreGeyser()
        let pond = Any?.Pond(source: source)
        defer { await pond.cancel() }
        await source.store.set("db/doc", to: ["a": "A0", "b": "B0"])
        var first = pond.stream("db", "doc", "a").makeAsyncIterator()
        var second = pond.stream("db", "doc", "b").makeAsyncIterator()
        #expect(try await first.next() as? String == "A0")
        #expect(try await second.next() as? String == "B0")
        #expect(await source.starts == 1)
        await pond.store.set("db", "doc", "a", to: "cache-only")
        await source.store.set("db/doc", to: ["a": "A1", "b": "B1"])
        #expect(try await first.next() as? String == "A1")
        #expect(try await second.next() as? String == "B1")
        #expect(await source.starts == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func a_lake_mounts_routed_ponds_preserving_full_routes_and_document_sharing() async throws {
        let localSource = RoutedSource()
        let remoteSource = RoutedSource()
        let local = Any?.Pond(source: localSource)
        let remote = Any?.Pond(source: remoteSource)
        defer { await local.cancel(); await remote.cancel() }
        typealias Documents = Lake<Any?.Route, Any?.Pond<RoutedSource>.Stream>
        let lake = try Documents(mounts: [
            .init(at: ["local"], source: { local.stream($0) }),
            .init(at: ["remote"], source: { remote.stream($0) }),
        ])
        let a: Any?.Route = ["local", "doc", "a"]
        let b: Any?.Route = ["local", "doc", "b"]
        let c: Any?.Route = ["remote", "doc", "c"]
        var first = try await lake.stream(a).makeAsyncIterator()
        var second = try await lake.stream(b).makeAsyncIterator()
        var third = try await lake.stream(c).makeAsyncIterator()
        try await routedWait { await localSource.mappingCount == 2 }
        try await routedWait { await remoteSource.mappingCount == 1 }
        await localSource.map(a, to: "local-document", at: ["local", "doc"])
        await localSource.map(b, to: "local-document", at: ["local", "doc"])
        await remoteSource.map(c, to: "remote-document", at: ["remote", "doc"])
        try await routedWait { await local.gushSources["local-document"]?.referenceCount == 2 }
        try await routedWait { await localSource.starts["local-document"] == 1 }
        try await routedWait { await remoteSource.starts["remote-document"] == 1 }
        await localSource.send("local-document", value: ["a": "A", "b": "B"])
        await remoteSource.send("remote-document", value: ["c": "C"])
        #expect(try await first.next() as? String == "A")
        #expect(try await second.next() as? String == "B")
        #expect(try await third.next() as? String == "C")
        #expect(await localSource.starts["local-document"] == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func remap_away_and_back_invalidates_a_delayed_cache_batch_without_a_new_gush() async throws {
        let source = RoutedSource()
        let store = Any?.Store(["doc": ["a": "baseline", "b": "baseline"]])
        let pond = Any?.Pond(source: source, store: store)
        let gate = RoutedGate()
        let nextGate = RoutedGate()
        defer { await gate.release(); await nextGate.release(); await pond.cancel() }
        let a: Any?.Route = ["doc", "a"]
        let b: Any?.Route = ["doc", "b"]
        let first = pond.stream(a).sink { _ in }
        let second = pond.stream(b).sink { _ in }
        defer { first.cancel(); second.cancel() }
        try await routedWait { await source.mappingCount == 2 }
        await source.map(a, to: "old", at: ["doc"])
        await source.map(b, to: "old", at: ["doc"])
        try await routedWait { await pond.gushSources["old"]?.referenceCount == 2 }
        try await routedWait { await source.starts["old"] == 1 }
        await pond.setCacheWriteBarrier { await gate.wait() }
        await source.send("old", value: ["a": "stale", "b": "stale"])
        var entered = gate.entered.makeAsyncIterator()
        _ = await entered.next()
        await source.map(a, to: "new", at: ["doc"])
        try await routedWait { await source.starts["new"] == 1 }
        await source.map(a, to: "old", at: ["doc"])
        try await routedWait { await pond.gushSources["old"]?.referenceCount == 2 }
        // Remounting replays the document into the cache under the new revision.
        try await routedWait { (try? await store.get("doc", "a", as: String.self)) == "stale" }
        await store.set("doc", "a", to: "remounted")
        // The older pending batch must not overwrite this newer mount.
        await pond.setCacheWriteBarrier { await nextGate.wait() }
        await source.send("old", value: ["a": "next", "b": "next"])
        await gate.release()
        var next = nextGate.entered.makeAsyncIterator()
        _ = await next.next()
        #expect(try await store.get("doc", "a", as: String.self) == "remounted")
        #expect(await source.starts["old"] == 1)
        await pond.setCacheWriteBarrier(nil)
        await nextGate.release()
    }
}

private enum RoutedFailure: Error { case upstream }
private actor RoutedSource: Geyser {
    typealias Gushes = AsyncThrowingStream<SendableValue, Error>
    typealias GushToRouteMapping = AsyncThrowingStream<(id: String, route: Any?.Route)?, Error>
    private var mappings: [Any?.Route: GushToRouteMapping.Continuation] = [:]
    private var outputs: [String: Gushes.Continuation] = [:]
    private let factoryGate: RoutedGate?
    private(set) var starts: [String: Int] = [:]
    private(set) var cancelledFactories = 0
    private(set) var endedMappings = 0
    private(set) var mappingCount = 0
    init(factoryGate: RoutedGate? = nil) { self.factoryGate = factoryGate }
    func source(of route: Any?.Route) -> GushToRouteMapping {
        let (stream, output) = GushToRouteMapping.makeStream()
        mappingCount += 1
        mappings[route] = output
        output.onTermination = { _ in Task { @concurrent in await self.mappingEnded() } }
        return stream
    }
    func stream(_ gush: String) async -> Gushes {
        starts[gush, default: 0] += 1
        let (stream, output) = Gushes.makeStream()
        outputs[gush] = output
        if let factoryGate, starts[gush] == 1 {
            await withTaskCancellationHandler {
                await factoryGate.wait()
            } onCancel: {
                Task { @concurrent in await self.recordCancellation() }
            }
        }
        return stream
    }
    func map(_ request: Any?.Route, to id: String?, at route: Any?.Route) {
        mappings[request]?.yield(id.map { ($0, route) })
    }
    func send(_ id: String, value: SendableValue) { outputs[id]?.yield(value) }
    func finish(_ id: String, error: Error? = nil) { outputs[id]?.finish(throwing: error) }
    func finishMapping(_ route: Any?.Route, error: Error? = nil) { mappings[route]?.finish(throwing: error) }
    private func mappingEnded() { endedMappings += 1 }
    private func recordCancellation() { cancelledFactories += 1 }
}

private actor RoutedGate {
    nonisolated let entered: AsyncStream<Void>
    private let output: AsyncStream<Void>.Continuation
    private var pending: CheckedContinuation<Void, Never>?
    init() { (entered, output) = AsyncStream.makeStream() }
    func wait() async {
        await withCheckedContinuation { continuation in pending = continuation; output.yield(()) }
    }
    func release() { pending?.resume(); pending = nil }
}
private func routedWait(_ condition: () async -> Bool) async throws {
    while !(await condition()) { try Task.checkCancellation(); await Task.yield() }
}

private actor StoreGeyser: Geyser {
    typealias Gushes = AsyncStream<SendableValue>
    typealias GushToRouteMapping = AsyncStream<(id: String, route: Any?.Route)?>
    let store = Any?.Store()
    private(set) var starts = 0
    func stream(_ gush: String) async -> Gushes { starts += 1; return await store.stream(.key(gush)) }
    func source(of route: Any?.Route) -> GushToRouteMapping {
        let root = Array(route.prefix(2))
        return AsyncStream { output in output.yield((root.joined(separator: "/"), root)); output.finish() }
    }
}
