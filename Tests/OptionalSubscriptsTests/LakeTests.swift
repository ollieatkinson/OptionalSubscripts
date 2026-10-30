@testable import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1)))
struct LakeTests {
    typealias Route = [String]
    typealias Routed = Lake<Route, AsyncStream<String?>>

    private static func values(_ values: String?...) -> AsyncStream<String?> {
        AsyncStream { continuation in
            for value in values { continuation.yield(value) }
            continuation.finish()
        }
    }

    @Test(arguments: [false, true])
    func deepest_prefix_wins_independent_of_registration_order(reversed: Bool) async throws {
        let mounts: [Routed.Mount<String>] = [
            .init(at: [], source: { _ in Self.values("default") }),
            .init(at: ["api"], source: { _ in Self.values("api") }),
            .init(at: ["api", "programme"], source: { request in Self.values(request.joined(separator: "/")) }),
        ]
        let lake = try Routed(mounts: reversed ? mounts.reversed() : mounts)
        let programme = try await lake.stream(["api", "programme", "p123"]).reduce(into: []) { $0.append($1) }
        let other = try await lake.stream(["apiary"]).reduce(into: []) { $0.append($1) }
        #expect(programme == ["api/programme/p123"])
        #expect(other == ["default"])
    }

    @Test func deep_routes_keep_the_deepest_prefix_and_original_request() async throws {
        let path = (0..<256).map { "segment-\($0)" }
        let lake = try Routed(mounts: [
            .init(at: Array(path.prefix(128)), source: { _ in Self.values("ancestor") }),
            .init(at: path, source: { Self.values($0.joined(separator: "/")) }),
            .init(at: [], source: { _ in Self.values("default") }),
            .init(at: Array(path.prefix(64)), source: { _ in Self.values("general") }),
        ])
        let child = path + ["leaf"]
        #expect(try await lake.stream(child).reduce(into: []) { $0.append($1) } == [child.joined(separator: "/")])
        let divergent = Array(path.prefix(200)) + ["other"]
        #expect(try await lake.stream(divergent).reduce(into: []) { $0.append($1) } == ["ancestor"])
    }

    @Test func wide_sibling_routes_match_complete_components() async throws {
        let siblings: [Routed.Mount<String>] = (0..<1_024).map { index in
            let name = "branch-\(index)"
            return .init(at: ["api", name], source: { request in Self.values(name, request.last) })
        }
        let lake = try Routed(mounts: siblings.reversed() + [
            .init(at: [], source: { _ in Self.values("default") }),
        ])
        for index in [0, 512, 1_023] {
            let name = "branch-\(index)"
            #expect(try await lake.stream(["api", name, "leaf"]).reduce(into: []) { $0.append($1) } == [name, "leaf"])
        }
        #expect(try await lake.stream(["api", "branch-10230"]).reduce(into: []) { $0.append($1) } == ["default"])
    }

    @Test func optional_subscript_routes_keep_keys_and_indices_distinct() async throws {
        typealias Native = Lake<Any?.Route, AsyncStream<String?>>
        let lake = try Native(mounts: [
            .init(at: ["items", 1], source: { _ in Self.values("index") }),
            .init(at: ["items", "1"], source: { _ in Self.values("key") }),
        ])
        #expect(try await lake.stream(["items", 1]).reduce(into: []) { $0.append($1) } == ["index"])
        #expect(try await lake.stream(["items", "1"]).reduce(into: []) { $0.append($1) } == ["key"])
    }

    @Test func duplicate_and_unmounted_routes_fail_before_opening_sources() async throws {
        let counter = Counter()
        let mount = Routed.Mount(at: ["api"]) { _ in
            await counter.increment()
            return Self.values("unexpected")
        }
        #expect(throws: Routed.RoutingError<String>.duplicateMount(["api"])) {
            try Routed(mounts: [mount, mount])
        }
        let root = Routed.Mount<String>(at: [], source: { _ in Self.values("default") })
        #expect(throws: Routed.RoutingError<String>.duplicateMount([])) {
            try Routed(mounts: [root, root])
        }
        let lake = try Routed(mounts: [mount])
        await #expect(throws: Routed.RoutingError<String>.unmounted(["other"])) {
            try await lake.stream(["other"])
        }
        #expect(await counter.value == 0)
    }

    @Test func selected_absence_and_empty_completion_do_not_fall_back() async throws {
        let counter = Counter()
        let lake = try Routed(mounts: [
            .init(at: [], source: { _ in
                await counter.increment()
                return Self.values("fallback")
            }),
            .init(at: ["missing"], source: { _ in Self.values(nil) }),
            .init(at: ["empty"], source: { _ in Self.values() }),
        ])
        let missing = try await lake.stream(["missing"]).reduce(into: []) { $0.append($1) }
        let empty = try await lake.stream(["empty"]).reduce(into: []) { $0.append($1) }
        #expect(missing.count == 1)
        #expect(missing[0] == nil)
        #expect(empty.isEmpty)
        #expect(await counter.value == 0)
    }

    @Test func opening_and_iteration_errors_preserve_the_original_error() async throws {
        enum Failure: Error { case opening, iteration }
        typealias Throwing = Lake<Route, AsyncThrowingStream<Int, Error>>
        let counter = Counter()
        let lake = try Throwing(mounts: [
            .init(at: [], source: { _ in
                await counter.increment()
                return AsyncThrowingStream { $0.finish() }
            }),
            .init(at: ["opening"], source: { _ in throw Failure.opening }),
            .init(at: ["iteration"], source: { _ in
                AsyncThrowingStream { continuation in
                    continuation.yield(1)
                    continuation.finish(throwing: Failure.iteration)
                }
            }),
        ])
        await #expect(throws: Failure.opening) { try await lake.stream(["opening"]) }
        var iterator = try await lake.stream(["iteration"]).makeAsyncIterator()
        #expect(try await iterator.next() == 1)
        await #expect(throws: Failure.iteration) { try await iterator.next() }
        #expect(await counter.value == 0)
    }

    @Test func nested_lakes_preserve_context_and_live_source_updates() async throws {
        struct Request: Sendable {
            let route: Route
            let entity: String
        }
        typealias Contextual = Lake<Request, AsyncStream<String?>>
        let store = [String: String].Store(["p123": "Dorothy"])
        // A typed provider can carry missing/pending/error states as values too.
        let inner = Contextual { request in
            await store.stream(request.entity)
        }
        let outer = try Contextual(mounts: [
            .init(at: ["api"], source: { try await inner.stream($0) }),
        ], route: { $0.route })
        var iterator = try await outer.stream(Request(route: ["api", "programme"], entity: "p123")).makeAsyncIterator()
        #expect(await iterator.next() == "Dorothy")
        await store.set("p123", to: "Witch")
        #expect(await iterator.next() == "Witch")
    }

    @Test func the_original_sequence_and_its_lifetime_are_preserved() async throws {
        final class Observations: AsyncSequence, Sendable {
            let values: AsyncStream<Int>
            init() { values = AsyncStream { $0.yield(42); $0.finish() } }
            func makeAsyncIterator() -> AsyncStream<Int>.Iterator { values.makeAsyncIterator() }
        }
        let original = Observations()
        let selected: Observations
        do {
            let lake = Lake<String, Observations> { _ in original }
            selected = try await lake.stream("first")
            let second = try await lake.stream("second")
            #expect(selected === original)
            #expect(second === original)
        }
        // Dropping the router must not close or cancel an already opened source.
        #expect(await selected.reduce(into: []) { $0.append($1) } == [42])
    }

    @Test func custom_routing_can_reject_ambiguity_without_opening_a_source() async throws {
        enum Failure: Error { case ambiguous }
        let counter = Counter()
        let lake = Routed { request in
            guard request == ["chosen"] else { throw Failure.ambiguous }
            await counter.increment()
            return Self.values("chosen")
        }
        await #expect(throws: Failure.ambiguous) { try await lake.stream(["ambiguous"]) }
        #expect(await counter.value == 0)
        let result = try await lake.stream(["chosen"]).reduce(into: []) { $0.append($1) }
        #expect(result == ["chosen"])
    }

    @Test func cancellation_reaches_the_original_sequence_without_a_forwarding_task() async throws {
        let (started, didStart) = AsyncStream<Void>.makeStream()
        let (terminated, didTerminate) = AsyncStream<Void>.makeStream()
        let lake = Lake<String, AsyncStream<Int>> { _ in
            AsyncStream { continuation in
                continuation.onTermination = { _ in
                    didTerminate.yield(())
                    didTerminate.finish()
                }
                didStart.yield(())
                didStart.finish()
            }
        }
        let reader = Task { @concurrent in
            for await _ in try await lake.stream("live") {}
        }
        for await _ in started { break }
        reader.cancel()
        for await _ in terminated { break }
        try await reader.value
    }

    @Test func cancelled_requests_do_not_open_sources() async throws {
        let counter = Counter()
        let lake = Routed { _ in
            await counter.increment()
            return Self.values("unexpected")
        }
        let task = Task { @concurrent in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await lake.stream([])
        }
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(await counter.value == 0)
    }

    @Test func cancellation_during_an_uncooperative_factory_rejects_its_result() async {
        actor Gate {
            let began: AsyncStream<Void>.Continuation
            var pending: CheckedContinuation<Void, Never>?
            init(began: AsyncStream<Void>.Continuation) { self.began = began }
            func wait() async {
                await withCheckedContinuation { continuation in
                    pending = continuation
                    began.yield(())
                    began.finish()
                }
            }
            func release() { pending?.resume(); pending = nil }
        }
        let (started, began) = AsyncStream<Void>.makeStream()
        let gate = Gate(began: began)
        let lake = Routed { _ in
            await gate.wait()
            return Self.values("too late")
        }
        let opening = Task { @concurrent in try await lake.stream([]) }
        for await _ in started { break }
        opening.cancel()
        await gate.release()
        await #expect(throws: CancellationError.self) { try await opening.value }
    }

    @Test func independent_sources_can_open_concurrently() async throws {
        actor Rendezvous {
            var waiting: CheckedContinuation<Void, Never>?
            var entered = 0
            func enter() async {
                entered += 1
                if entered == 2 {
                    waiting?.resume()
                } else {
                    await withCheckedContinuation { waiting = $0 }
                }
            }
        }
        let gate = Rendezvous()
        let lake = Routed { request in
            await gate.enter()
            return Self.values(request.first)
        }
        async let left = lake.stream(["left"])
        async let right = lake.stream(["right"])
        let results = try await (left, right)
        #expect(await results.0.reduce(into: []) { $0.append($1) } == ["left"])
        #expect(await results.1.reduce(into: []) { $0.append($1) } == ["right"])
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func composed_requests_share_one_value_pond_and_its_cancellation() async throws {
        let counter = Counter()
        let (upstream, continuation) = AsyncStream<Int>.makeStream()
        let pond = ValuePond(cancellingGracePeriod: .zero, source: {
            await counter.increment()
            return upstream
        })
        defer { await pond.cancel() }
        let lake = try Lake<Route, AsyncStream<Int>>(mounts: [
            .init(at: ["data"], source: { _ in await pond.stream() }),
        ])
        var first = try await lake.stream(["data", "first"]).makeAsyncIterator()
        var second = try await lake.stream(["data", "second"]).makeAsyncIterator()
        #expect(await pond.referenceCount == 2)
        continuation.yield(42)
        #expect(await first.next() == 42)
        #expect(await second.next() == 42)
        #expect(await counter.value == 1)
        await pond.cancel()
        #expect(await first.next() == nil)
        #expect(await second.next() == nil)
    }

    private actor Counter {
        var value = 0
        func increment() { value += 1 }
    }
}
