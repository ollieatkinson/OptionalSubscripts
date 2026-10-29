#if canImport(Combine)
import Combine
import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1))) struct CombineTests {
    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @MainActor @Test func main_actor_publisher_supports_standard_operators_and_erasure() async throws {
        let store = [String: Int].Store()
        let (received, output) = AsyncStream<[Int?]>.makeStream()
        let publisher = await store.publisher(for: "x", bufferingPolicy: .unbounded)
            .map { $0.map { $0 * 2 } }.prefix(3).collect().eraseToAnyPublisher()
        let observation = publisher.sink { values in output.yield(values); output.finish() }
        defer { observation.cancel() }
        await store.set("x", to: 1)
        await store.set("x", to: 2)
        var values = received.makeAsyncIterator()
        #expect(await values.next() == [nil, 2, 4])
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @MainActor @Test func explicit_demand_resumes_the_same_iterator_and_cancellation_stops_delivery() async {
        let (source, output) = AsyncStream<Int>.makeStream()
        let (terminated, stopped) = AsyncStream<Bool>.makeStream()
        output.onTermination = { reason in
            if case .cancelled = reason { stopped.yield(true) }
            else { stopped.yield(false) }
            stopped.finish()
        }
        let consumer = DemandConsumer()
        let publisher = source.publisher()
        defer { withExtendedLifetime(publisher) {} }
        publisher.subscribe(consumer)
        output.yield(1); output.yield(2); output.yield(3)
        consumer.subscription?.request(.max(1))
        var events = consumer.events.makeAsyncIterator()
        #expect(await events.next() == 1)
        consumer.subscription?.request(.max(1))
        #expect(await events.next() == 2)
        consumer.subscription?.cancel()
        output.yield(4)
        // Cancellation reaches the source iterator without issuing new demand.
        var cancellation = terminated.makeAsyncIterator()
        #expect(await cancellation.next() == true)
        #expect(consumer.values == [1, 2])
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @MainActor @Test func cancellation_before_any_demand_releases_a_retained_sequence() async {
        let (source, output) = AsyncStream<Int>.makeStream()
        let (terminated, stopped) = AsyncStream<Bool>.makeStream()
        output.onTermination = { reason in
            if case .cancelled = reason { stopped.yield(true) } else { stopped.yield(false) }
            stopped.finish()
        }
        let publisher = source.publisher()
        defer { withExtendedLifetime(publisher) {} }
        let consumer = DemandConsumer()
        publisher.subscribe(consumer)
        consumer.subscription?.cancel()
        var cancellation = terminated.makeAsyncIterator()
        #expect(await cancellation.next() == true)
        #expect(consumer.values.isEmpty)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @MainActor @Test func publisher_preserves_source_failure_type_and_completion() async {
        enum Failure: Error { case source }
        let (source, output) = AsyncThrowingStream<Int, Error>.makeStream()
        let (completions, finished) = AsyncStream<Bool>.makeStream()
        let observation = source.publisher().sink { completion in
            if case .failure(let error) = completion {
                finished.yield(error is Failure)
                finished.finish()
            }
        } receiveValue: { _ in }
        defer { observation.cancel() }
        output.finish(throwing: Failure.source)
        var values = completions.makeAsyncIterator()
        #expect(await values.next() == true)
    }
}

@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
@MainActor private final class DemandConsumer: @MainActor Subscriber {
    typealias Input = Int
    typealias Failure = Never
    var subscription: (any Subscription)?
    var values: [Int] = []
    let events: AsyncStream<Int>
    private let output: AsyncStream<Int>.Continuation
    init() { (events, output) = AsyncStream.makeStream() }
    func receive(subscription: any Subscription) { self.subscription = subscription }
    func receive(_ input: Int) -> Subscribers.Demand {
        values.append(input); output.yield(input); return .none
    }
    func receive(completion: Subscribers.Completion<Never>) { output.finish() }

}
#endif
