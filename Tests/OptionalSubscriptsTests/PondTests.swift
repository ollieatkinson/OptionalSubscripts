import Clocks
import Synchronization
@testable import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1))) struct PondTests {
    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func mapped_sequences_preserve_values_and_share_the_underlying_source() async {
        let source = PondSource()
        let pond = Pond(source: { await source.open().map { $0 * 2 } })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        var first = await pond.stream().makeAsyncIterator()
        var second = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))
        for value in [3, 1, 4] {
            await source.send(value, to: 1)
            #expect(await first.next() == value * 2)
            #expect(await second.next() == value * 2)
        }
        #expect(await source.starts == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func releasing_the_owner_finishes_observers_and_cancels_upstream() async {
        let source = PondSource()
        var pond: Pond<AsyncStream<Int>>? = Pond(source: { await source.open() })
        var events = source.events.makeAsyncIterator()
        var values = await pond!.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))

        pond = nil
        #expect(await values.next() == nil)
        #expect(await events.next() == .closed(1))
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func cancelling_get_releases_its_pending_subscription() async {
        let source = PondSource()
        let pond = Pond(cancellingGracePeriod: .zero, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let task = Task { @concurrent in try await pond.get() }
        #expect(await events.next() == .opened(1))

        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(await events.next() == .closed(1))
        #expect(await pond.referenceCount == 0)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func pending_subscribers_share_one_source_and_late_subscribers_get_replay() async {
        let source = PondSource()
        let pond = Pond(source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        var first = await pond.stream().makeAsyncIterator()
        var second = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))
        #expect(await source.starts == 1)

        await source.send(7, to: 1)
        #expect(await first.next() == 7)
        #expect(await second.next() == 7)
        var late = await pond.stream().makeAsyncIterator()
        #expect(await late.next() == 7)
        await source.send(8, to: 1)
        #expect(await first.next() == 8)
        #expect(await second.next() == 8)
        #expect(await late.next() == 8)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func cancelling_before_the_first_value_preserves_the_remaining_subscriber() async throws {
        let source = PondSource()
        let pond = Pond(cancellingGracePeriod: .zero, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let stream = await pond.stream()
        let task = Task { @concurrent in for await _ in stream {} }
        var remaining = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))
        task.cancel()
        await task.value
        try await waitUntil { await pond.referenceCount == 1 }

        await source.send(1, to: 1)
        #expect(await remaining.next() == 1)
        #expect(await source.starts == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func the_final_cancellation_releases_a_source_waiting_for_its_first_value() async {
        let source = PondSource()
        let pond = Pond(cancellingGracePeriod: .zero, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let stream = await pond.stream()
        let task = Task { @concurrent in for await _ in stream {} }
        #expect(await events.next() == .opened(1))
        task.cancel()
        await task.value
        #expect(await events.next() == .closed(1))
        #expect(await pond.referenceCount == 0)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func completed_sources_keep_subscribers_and_replay_until_they_leave() async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        var values = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))
        await source.send(42, to: 1)
        await source.finish(1)
        #expect(await values.next() == 42)
        await clock.advance(by: .seconds(20))
        #expect(try await pond.get() == 42)
        #expect(await source.starts == 1)

        await pond.cancel()
        #expect(await values.next() == nil)
        var restarted = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .closed(1))
        #expect(await events.next() == .opened(2))
        await source.send(99, to: 2)
        #expect(await restarted.next() == 99)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func one_shot_get_refreshes_only_after_the_grace_period() async throws {
        let counter = PondCounter()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(20), clock: clock, get: { await counter.next() })
        defer { await pond.cancel() }

        #expect(try await pond.get() == 1)
        try await waitUntil { await pond.referenceCount == 0 }
        await clock.advance(by: .seconds(5))
        #expect(try await pond.get() == 1)
        try await waitUntil { await pond.referenceCount == 0 }
        await clock.advance(by: .seconds(20))
        #expect(try await pond.get() == 2)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func get_reads_the_latest_value_while_upstream_is_retained() async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let first = Task { @concurrent in try await pond.get() }
        #expect(await events.next() == .opened(1))
        await source.send(0, to: 1)
        #expect(try await first.value == 0)

        for value in [10, 100] {
            try await waitUntil { await pond.referenceCount == 0 }
            await source.send(value, to: 1)
            await clock.advance(by: .seconds(1))
            #expect(try await pond.get() == value)
        }
        #expect(await source.starts == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test(arguments: [false, true])
    func cancellation_waits_for_the_full_grace_period(using_get: Bool) async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let task = Task { @concurrent in
            if using_get { _ = try? await pond.get() }
            else { for await _ in await pond.stream() {} }
        }
        #expect(await events.next() == .opened(1))
        task.cancel()
        await task.value
        try await waitUntil { await pond.referenceCount == 0 }

        await clock.advance(by: .seconds(3))
        #expect(source.terminations.count == 0)
        await clock.advance(by: .seconds(2))
        #expect(await events.next() == .closed(1))
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func multiple_cancellations_start_the_deadline_only_after_the_last_subscriber() async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let tasks = (0..<10).map { _ in Task { @concurrent in for await _ in await pond.stream() {} } }
        #expect(await events.next() == .opened(1))
        try await waitUntil { await pond.referenceCount == 10 }

        for (index, task) in tasks.dropLast().enumerated() {
            task.cancel()
            await task.value
            try await waitUntil { await pond.referenceCount == 9 - index }
        }
        await clock.advance(by: .seconds(10))
        #expect(source.terminations.count == 0)
        tasks.last?.cancel()
        await tasks.last?.value
        try await waitUntil { await pond.referenceCount == 0 }
        await clock.advance(by: .seconds(3))
        #expect(source.terminations.count == 0)
        await clock.advance(by: .seconds(2))
        #expect(await events.next() == .closed(1))
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func a_timed_source_delivers_every_value_and_restarts_from_the_beginning() async throws {
        let clock = TestClock()
        let source = PondTimer(clock: clock)
        let collected = PondValues()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var starts = source.starts.makeAsyncIterator()
        let first = Task { @concurrent in for await value in await pond.stream() { await collected.append(value) } }
        #expect(await starts.next() == 1)
        for (advance, total) in [(1, 1), (2, 3), (2, 5), (15, 20)] {
            await clock.advance(by: .seconds(advance))
            try await waitUntil { await collected.values.count >= total }
            #expect(await collected.values == Array(1...total))
            #expect(try await pond.get() == total)
        }
        first.cancel()
        await first.value
        try await waitUntil { await pond.referenceCount == 0 }
        await clock.advance(by: .seconds(10))
        #expect(await collected.values == Array(1...20))

        let second = Task { @concurrent in for await value in await pond.stream() { await collected.append(value) } }
        #expect(await starts.next() == 2)
        await clock.advance(by: .seconds(10))
        try await waitUntil { await collected.values.count >= 30 }
        #expect(await collected.values == Array(1...20) + Array(1...10))
        second.cancel()
        await second.value
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func get_cancelled_before_acquisition_does_not_start_a_source() async {
        let source = PondSource()
        let gate = PondGate()
        let pond = Pond(source: { await source.open() })
        defer { await pond.cancel() }
        var entered = gate.entered.makeAsyncIterator()
        let task = Task { @concurrent in
            await gate.wait()
            return try await pond.get()
        }
        #expect(await entered.next() != nil)
        task.cancel()
        await gate.release()
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(await source.starts == 0)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func empty_completion_and_optional_nil_are_distinct() async throws {
        let empty = Pond(source: { AsyncStream<Int> { $0.finish() } })
        defer { await empty.cancel() }
        await #expect(throws: Pond<AsyncStream<Int>>.GetError.finishedWithoutValue) {
            try await empty.get()
        }
        let optional = Pond(get: { Optional<Int>.none })
        defer { await optional.cancel() }
        let value = try await optional.get()
        #expect(value == nil)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func subscriber_buffering_applies_without_an_unbounded_forwarding_stream() async {
        let source = PondSource()
        let pond = Pond(source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        var slow = await pond.stream(bufferingPolicy: .bufferingNewest(1)).makeAsyncIterator()
        var fast = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(1))
        await source.send(1, to: 1)
        #expect(await fast.next() == 1)
        await source.send(2, to: 1)
        #expect(await fast.next() == 2)
        #expect(await slow.next() == 2)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func grace_expiry_releases_upstream_and_discards_old_replay() async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let stream = await pond.stream()
        let task = Task { @concurrent in for await _ in stream {} }
        #expect(await events.next() == .opened(1))
        await source.send(1, to: 1)
        // A second consumer confirms that replay has actually been populated.
        #expect(try await pond.get() == 1)
        task.cancel()
        await task.value
        try await waitUntil { await pond.referenceCount == 0 }
        await clock.run()
        #expect(await events.next() == .closed(1))

        var restarted = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(2))
        await source.send(2, to: 2)
        #expect(await restarted.next() == 2)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func a_returning_subscriber_invalidates_the_old_grace_deadline() async throws {
        let source = PondSource()
        let clock = TestClock()
        let pond = Pond(cancellingGracePeriod: .seconds(5), clock: clock, source: { await source.open() })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        let stream = await pond.stream()
        let task = Task { @concurrent in for await _ in stream {} }
        #expect(await events.next() == .opened(1))
        task.cancel()
        await task.value
        try await waitUntil { await pond.referenceCount == 0 }

        var returning = await pond.stream().makeAsyncIterator()
        await clock.advance(by: .seconds(10))
        await source.send(2, to: 1)
        #expect(await returning.next() == 2)
        #expect(await source.starts == 1)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func a_cancelled_factory_cannot_publish_into_a_new_generation() async {
        let source = PondSource()
        let gate = PondGate()
        let pond = Pond(cancellingGracePeriod: .zero, source: {
            let (id, stream) = await source.openWithID()
            if id == 1 { await gate.wait() }
            return stream
        })
        defer { await pond.cancel() }
        var events = source.events.makeAsyncIterator()
        var entered = gate.entered.makeAsyncIterator()
        var old = await pond.stream().makeAsyncIterator()
        #expect(await entered.next() != nil)
        #expect(await events.next() == .opened(1))
        await pond.cancel()
        #expect(await old.next() == nil)
        var current = await pond.stream().makeAsyncIterator()
        #expect(await events.next() == .opened(2))
        await source.send(2, to: 2)
        #expect(await current.next() == 2)

        await source.send(100, to: 1)
        await gate.release()
        #expect(await events.next() == .closed(1))
        await source.send(3, to: 2)
        #expect(await current.next() == 3)
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private actor PondSource {
    enum Event: Equatable, Sendable { case opened(Int), closed(Int) }
    nonisolated let events: AsyncStream<Event>
    nonisolated let terminations = PondTerminations()
    private let continuation: AsyncStream<Event>.Continuation
    private var outputs: [Int: AsyncStream<Int>.Continuation] = [:]
    private(set) var starts = 0

    init() { (events, continuation) = AsyncStream.makeStream() }

    func open() -> AsyncStream<Int> { openWithID().1 }

    func openWithID() -> (Int, AsyncStream<Int>) {
        starts += 1
        let id = starts
        let (stream, output) = AsyncStream.makeStream(of: Int.self)
        output.onTermination = { [continuation, terminations] _ in
            terminations.record()
            continuation.yield(.closed(id))
        }
        outputs[id] = output
        continuation.yield(.opened(id))
        return (id, stream)
    }

    func send(_ value: Int, to id: Int) { outputs[id]?.yield(value) }
    func finish(_ id: Int) { outputs.removeValue(forKey: id)?.finish() }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private final class PondTerminations: Sendable {
    private let storage = Mutex(0)
    var count: Int { storage.withLock { $0 } }
    func record() { storage.withLock { $0 += 1 } }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private actor PondCounter {
    private var value = 0
    func next() -> Int { value += 1; return value }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private actor PondValues {
    private(set) var values: [Int] = []
    func append(_ value: Int) { values.append(value) }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private actor PondTimer {
    nonisolated let starts: AsyncStream<Int>
    private let started: AsyncStream<Int>.Continuation
    private let clock: TestClock<Duration>
    private var count = 0

    init(clock: TestClock<Duration>) {
        self.clock = clock
        (starts, started) = AsyncStream.makeStream()
    }

    func open() -> AsyncStream<Int> {
        count += 1
        let origin = clock.now
        let stream = AsyncStream<Int> { continuation in
            let task = Task { @concurrent [clock] in
                do {
                    for value in 1...1000 {
                        try await clock.sleep(until: origin.advanced(by: .seconds(value)))
                        continuation.yield(value)
                    }
                } catch {}
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
        started.yield(count)
        return stream
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
private actor PondGate {
    nonisolated let entered: AsyncStream<Void>
    private let signal: AsyncStream<Void>.Continuation
    private var continuation: CheckedContinuation<Void, Never>?

    init() { (entered, signal) = AsyncStream.makeStream() }

    func wait() async {
        await withCheckedContinuation { continuation in
            self.continuation = continuation
            signal.yield(())
        }
    }

    func release() {
        continuation?.resume()
        continuation = nil
    }
}

// Stream cancellation unregisters through an actor hop. Wait for that specific
// lifecycle condition before advancing virtual time, without wall-clock sleeps.
private func waitUntil(_ predicate: () async -> Bool) async throws {
    while !(await predicate()) {
        try Task.checkCancellation()
        await Task.yield()
    }
}
