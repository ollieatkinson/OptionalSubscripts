#if canImport(Combine)
import Combine

// Synchronous Combine operators need no cross-isolation Subscriber transfer.
public extension Publisher {
    func filter<Value>(_: Value.Type = Value.self) -> Publishers.CompactMap<Self, Value> {
        compactMap { $0 as? Value }
    }

    func cast<Value>(to: Value.Type = Value.self) -> Publishers.TryMap<Self, Value> {
        tryMap { value in
            guard let result = value as? Value else { throw CastingError(value: value, to: Value.self) }
            return result
        }
    }
}
#endif

/// Owns a sequence observation. Releasing or cancelling it cancels its task.
public final class AsyncObservation: Sendable {
    private let task: Task<Void, Never>
    init(task: Task<Void, Never>) { self.task = task }
    deinit { task.cancel() }
    public func cancel() { task.cancel() }
    public func wait() async { await task.value }
}

public extension AsyncSequence where Self: Sendable, Element: Sendable {
    /// Observe serially in a task, using native async operators before `sink`.
    /// Handlers must be Sendable; this is not a Combine Publisher conversion.
    func sink(
        receiveCompletion: @escaping @Sendable (Result<Void, Error>) async -> Void = { _ in },
        receiveValue: @escaping @Sendable (Element) async -> Void
    ) -> AsyncObservation {
        AsyncObservation(task: Task { @concurrent in
            do {
                for try await value in self {
                    try Task.checkCancellation()
                    await receiveValue(value)
                }
                try Task.checkCancellation()
                await receiveCompletion(.success(()))
            } catch {
                await receiveCompletion(.failure(error))
            }
        })
    }
}

#if canImport(Combine)
/// A real Combine publisher whose subscriber and subscription remain on MainActor.
/// Use native async observation for background consumers. Isolated conformance
/// prevents this publisher's Combine operations from being used off MainActor.
@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public struct AsyncSequencePublisher<Sequence: AsyncSequence & Sendable>: @MainActor Publisher, Sendable
where Sequence.Element: Sendable {
    public typealias Output = Sequence.Element
    public typealias Failure = Sequence.Failure
    private let sequence: Sequence
    public init(_ sequence: Sequence) { self.sequence = sequence }

    @MainActor public func receive<Consumer: Subscriber>(subscriber: Consumer)
    where Consumer.Input == Output, Consumer.Failure == Failure {
        subscriber.receive(subscription: SequenceSubscription(sequence: sequence, subscriber: subscriber))
    }

    @MainActor private final class SequenceSubscription<Consumer: Subscriber>: @MainActor Subscription
    where Consumer.Input == Output, Consumer.Failure == Failure {
        private var subscriber: Consumer?
        private var demand: Subscribers.Demand = .none
        private var task: Task<Void, Never>?
        private var waiter: CheckedContinuation<Void, Never>?

        init(sequence: Sequence, subscriber: Consumer) {
            self.subscriber = subscriber
            // The iterator and UI-owned demand state stay on this one task.
            task = Task { @MainActor in await self.pump(sequence) }
        }
        func request(_ additional: Subscribers.Demand) {
            guard subscriber != nil, additional > .none else { return }
            demand += additional
            waiter?.resume(); waiter = nil
        }
        private func pump(_ sequence: Sequence) async {
            var iterator = sequence.makeAsyncIterator()
            var completed = false
            defer {
                // Cancellation can occur while waiting for demand, outside
                // next(). Give the same iterator its cancellation handshake.
                // No new value is delivered and no concurrent next is issued.
                if Task.isCancelled, !completed {
                    _ = try? await iterator.next(isolation: MainActor.shared)
                }
            }
            while subscriber != nil, !Task.isCancelled {
                if demand == .none {
                    await withCheckedContinuation { self.waiter = $0 }
                    continue
                }
                do throws(Failure) {
                    guard let value = try await iterator.next(isolation: MainActor.shared) else {
                        completed = true
                        complete(.finished)
                        return
                    }
                    guard !Task.isCancelled, let subscriber else { return }
                    demand -= 1
                    demand += subscriber.receive(value)
                } catch {
                    completed = true
                    if !Task.isCancelled { complete(.failure(error)) }
                    return
                }
            }
        }
        func cancel() {
            subscriber = nil
            task?.cancel(); task = nil
            waiter?.resume(); waiter = nil
        }
        private func complete(_ completion: Subscribers.Completion<Failure>) {
            let subscriber = self.subscriber
            cancel()
            subscriber?.receive(completion: completion)
        }
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public extension AsyncSequence where Self: Sendable, Element: Sendable {
    @MainActor func publisher() -> AsyncSequencePublisher<Self> { .init(self) }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public extension Optional.Store where Wrapped == Any {
    @MainActor func publisher(for route: Location..., bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) async -> AsyncSequencePublisher<AsyncStream<SendableValue>> {
        await stream(route, bufferingPolicy: bufferingPolicy).publisher()
    }
    @MainActor func publisher(for route: Route, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) async -> AsyncSequencePublisher<AsyncStream<SendableValue>> {
        await stream(route, bufferingPolicy: bufferingPolicy).publisher()
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public extension Dictionary.Store {
    @MainActor func publisher(for key: Key, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) async -> AsyncSequencePublisher<AsyncStream<Value?>> {
        await stream(key, bufferingPolicy: bufferingPolicy).publisher()
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
public extension Optional.Pond where Wrapped == Any {
    @MainActor func publisher(for route: Location..., bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> AsyncSequencePublisher<Stream> {
        stream(route, bufferingPolicy: bufferingPolicy).publisher()
    }
    @MainActor func publisher(for route: Route, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> AsyncSequencePublisher<Stream> {
        stream(route, bufferingPolicy: bufferingPolicy).publisher()
    }
}
#endif
