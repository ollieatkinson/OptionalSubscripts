#if compiler(>=6.0)
import Foundation

/// Shares one upstream sequence, replaying its latest value to each subscriber.
///
/// Adapted from Oliver Atkinson's ValuePond:
/// https://gist.github.com/ollieatkinson/9f2748231612483e0cfd5c2cf76d44e9
/// Behaviour checked against revision 364b12981c80cda9008581cf2521de4ecdf8d7f1.
///
/// Subscription starts when `stream` or `get` is called. The source stays alive
/// while observed and for the grace period afterwards. A completed source's last
/// value remains live while subscribed, matching the original Pond. Expiry clears replay;
/// the next subscriber starts a fresh source. Errors can be carried as Result or
/// snapshot values, keeping recoverable failures inside the stream.
@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public actor ValuePond<Source: AsyncSequence & Sendable>
where Source.Element: Sendable, Source.Failure == Never {
    public typealias Value = Source.Element
    public typealias BufferingPolicy = AsyncStream<Value>.Continuation.BufferingPolicy

    private enum Replay {
        case empty
        case value(Value)
    }

    private let source: @Sendable () async -> Source
    private let gracePeriod: Duration
    private let sleep: @Sendable () async throws -> Void
    private var replay = Replay.empty
    private var observers: [UUID: AsyncStream<Value>.Continuation] = [:]
    private var upstream: Task<Void, Never>?
    private var graceTask: Task<Void, Never>?
    private var generation: UInt64 = 0
    private var leaseVersion: UInt64 = 0
    private var completed = false

    // Useful when diagnosing source lifetimes, without exposing continuations.
    var referenceCount: Int { observers.count }

    public init(
        cancellingGracePeriod: Duration = .seconds(60),
        clock: some Clock<Duration> = ContinuousClock(),
        source: @escaping @Sendable () async -> Source
    ) {
        self.source = source
        self.gracePeriod = max(.zero, cancellingGracePeriod)
        self.sleep = { try await clock.sleep(for: cancellingGracePeriod) }
    }

    deinit {
        upstream?.cancel()
        graceTask?.cancel()
        for continuation in observers.values { continuation.finish() }
    }

    public func stream(bufferingPolicy: BufferingPolicy = .unbounded) -> AsyncStream<Value> {
        let (stream, continuation) = AsyncStream.makeStream(of: Value.self, bufferingPolicy: bufferingPolicy)
        let id = UUID()
        continuation.onTermination = { @Sendable [weak self] _ in
            Task { await self?.remove(id) }
        }
        graceTask?.cancel()
        graceTask = nil
        leaseVersion &+= 1

        if completed, case .empty = replay {
            continuation.finish()
            scheduleRelease()
            return stream
        }

        observers[id] = continuation
        if case .value(let value) = replay { continuation.yield(value) }
        if upstream == nil, !completed { start() }
        return stream
    }

    public func get() async throws -> Value {
        try Task.checkCancellation()
        var iterator = stream().makeAsyncIterator()
        let value = await iterator.next()
        try Task.checkCancellation()
        guard let value else { throw GetError.finishedWithoutValue }
        return value
    }

    public enum GetError: Error, Equatable, Sendable {
        case finishedWithoutValue
    }

    /// Cancels the current source, closes subscribers and clears replay.
    /// A subsequent subscription starts a fresh source.
    public func cancel() {
        let continuations = observers.values
        observers.removeAll()
        reset()
        for continuation in continuations { continuation.finish() }
    }

    private func start() {
        generation &+= 1
        let generation = generation
        let source = source
        upstream = Task { [weak self] in
            let sequence = await source()
            guard !Task.isCancelled else { return }
            for await value in sequence {
                guard !Task.isCancelled else { break }
                await self?.publish(value, generation: generation)
            }
            await self?.finish(generation: generation)
        }
    }

    private func publish(_ value: Value, generation: UInt64) {
        guard self.generation == generation else { return }
        replay = .value(value)
        for continuation in observers.values { continuation.yield(value) }
    }

    private func finish(generation: UInt64) {
        guard self.generation == generation else { return }
        upstream = nil
        completed = true
        if case .empty = replay {
            let continuations = observers.values
            observers.removeAll()
            for continuation in continuations { continuation.finish() }
        }
        if observers.isEmpty { scheduleRelease() }
    }

    private func remove(_ id: UUID) {
        guard observers.removeValue(forKey: id) != nil else { return }
        if observers.isEmpty { scheduleRelease() }
    }

    private func scheduleRelease() {
        graceTask?.cancel()
        leaseVersion &+= 1
        guard gracePeriod > .zero else { return reset() }
        let version = leaseVersion
        graceTask = Task { [weak self, sleep] in
            do { try await sleep() } catch { return }
            guard !Task.isCancelled else { return }
            await self?.release(if: version)
        }
    }

    private func release(if version: UInt64) {
        // Check and cancellation share one actor turn, so a new subscriber
        // cannot arrive between testing the lease and cancelling its source.
        guard leaseVersion == version, observers.isEmpty else { return }
        reset()
    }

    private func reset() {
        generation &+= 1
        leaseVersion &+= 1
        upstream?.cancel()
        upstream = nil
        graceTask?.cancel()
        graceTask = nil
        completed = false
        replay = .empty
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public extension ValuePond {
    init<Value: Sendable>(
        cancellingGracePeriod: Duration = .seconds(60),
        clock: some Clock<Duration> = ContinuousClock(),
        get: @escaping @Sendable () async -> Value
    ) where Source == AsyncStream<Value> {
        self.init(cancellingGracePeriod: cancellingGracePeriod, clock: clock) {
            AsyncStream { continuation in
                let task = Task {
                    let value = await get()
                    guard !Task.isCancelled else { return }
                    continuation.yield(value)
                    continuation.finish()
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }
}

#endif
