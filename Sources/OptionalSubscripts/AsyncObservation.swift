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
    /// Handlers must be Sendable and can suspend between values.
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
