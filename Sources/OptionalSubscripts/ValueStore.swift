/// ValueStore is an actor-based store for sharing a single value.
/// The value can be shared safely across actor contexts and is available as a one-off get `value` or consumed as a stream of all changes to the value.
///
/// Each new consumer of `stream(bufferingPolicy:)` will receive it's own unique continuation.
///
/// `ValueStore` is data-race free and is built with concurrency in mind.
///
/// # Example
/// A store of names, showing get, set and stream.
///
/// ```swift
/// let store = ValueStore("Oliver")
/// await store.value // "Oliver
///
/// await store.set(to: "Vincent")
/// await store.value // Vincent
///
/// let task = Task { @concurrent in
///   for await name in store.stream().prefix(2) {
///     print("🔍", name)
///   }
/// }
///
/// await store.set(to: "Thomas")
/// await task.value
///
/// // 🔍 Vincent
/// // 🔍 Thomas
/// ```
public actor ValueStore<Value: Sendable> {
    public var value: Value

    private(set) var count: UInt = 0
    private var continuations: [UInt: AsyncStream<Value>.Continuation] = [:]

    /// Initializes a new instance of `ValueStore` with the specified initial value.
    ///
    /// - Parameter initialValue: The initial value to be stored in the `ValueStore`.
    public init(_ initialValue: Value) {
        self.value = initialValue
    }
}

public extension ValueStore {
    /// Sets the value of the `ValueStore` to the specified new value.
    ///
    /// - Parameter newValue: The new value to be set.
    func set(to newValue: Value) {
        value = newValue
        for (_, continuation) in continuations {
            continuation.yield(newValue)
        }
    }

    /// Performs an inout operation on the value of the `ValueStore`.
    ///
    /// - Parameter apply: The inout operation to be performed on the value.
    func `inout`(_ apply: @Sendable (inout Value) -> Void) {
        apply(&value)
        for (_, continuation) in continuations {
            continuation.yield(value)
        }
    }

    /// A strategy that handles exhaustion of a buffer’s capacity.
    typealias BufferingPolicy = AsyncStream<Value>.Continuation.BufferingPolicy

    /// Creates and returns an `AsyncStream` that streams the values stored in the `ValueStore`.
    ///
    /// - Parameter bufferingPolicy: The buffering policy to be used for the stream. Default is `.unbounded`.
    /// - Returns: An `AsyncStream` that streams the values stored in the `ValueStore`.
    func stream(bufferingPolicy: BufferingPolicy = .unbounded) -> AsyncStream<Value> {
        AsyncStream(bufferingPolicy: bufferingPolicy) { continuation in insert(continuation) }
    }
}

private extension ValueStore {
    func insert(_ continuation: AsyncStream<Value>.Continuation) {
        continuation.yield(value)
        let id = count + 1
        count = id
        continuations[id] = continuation
        continuation.onTermination = { @Sendable [weak self] _ in
            guard let self else { return }
            Task { @concurrent in await self.remove(continuation: id) }
        }
    }

    func remove(continuation id: UInt) {
        continuations.removeValue(forKey: id)
    }
}

extension ValueStore: Equatable {
    public static func == (lhs: ValueStore, rhs: ValueStore) -> Bool {
        lhs === rhs
    }
}
