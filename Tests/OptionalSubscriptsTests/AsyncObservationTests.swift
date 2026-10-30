import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1))) struct AsyncObservationTests {
    @Test func releasing_the_observation_cancels_a_retained_upstream_sequence() async {
        let (source, output) = AsyncStream<Int>.makeStream()
        let (terminated, stopped) = AsyncStream<Bool>.makeStream()
        output.onTermination = { reason in
            if case .cancelled = reason { stopped.yield(true) }
            else { stopped.yield(false) }
            stopped.finish()
        }
        var observation: AsyncObservation? = source.sink { _ in }
        weak let owner = observation
        observation = nil
        #expect(owner == nil)
        var cancellation = terminated.makeAsyncIterator()
        #expect(await cancellation.next() == true)
        withExtendedLifetime(source) {}
    }

    @Test func sink_delivers_values_then_the_original_source_failure() async {
        enum Failure: Error { case upstream }
        let (source, output) = AsyncThrowingStream<Int, Error>.makeStream()
        let (completions, completed) = AsyncStream<Bool>.makeStream()
        let received = ValueStore([Int]())
        let observation = source.sink { result in
            if case .failure(let error) = result { completed.yield(error is Failure) }
            else { completed.yield(false) }
            completed.finish()
        } receiveValue: { value in
            await received.inout { $0.append(value) }
        }
        output.yield(7)
        output.finish(throwing: Failure.upstream)
        await observation.wait()
        #expect(await received.value == [7])
        var completion = completions.makeAsyncIterator()
        #expect(await completion.next() == true)
    }
}
