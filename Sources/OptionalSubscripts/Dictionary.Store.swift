public extension Dictionary where Key: Sendable, Value: Sendable {
    /// Dictionary.Store is an actor-based store for sharing a collection of values that provides transactional updates and asynchronous notifications of changes.
    ///
    /// The store provides methods for inserting, removing, and updating values in the dictionary. It also supports asynchronous streaming of values for a given key.
    ///  It is an actor, which means it can be safely accessed from multiple concurrent tasks. It maintains a transaction level to ensure atomicity of updates. Transactions can be started using the `transaction` method, and updates within a transaction are batched and applied atomically.
    ///
    /// # Example
    ///
    /// ```swift
    /// let store = [String: Int].Store(["x": 1, "y": 0])
    /// try await store.get("x") // 1
    ///
    /// await store.set("x", to: 0)
    /// try await store.get("x") // 0
    ///
    /// let x = Task { @concurrent in
    ///  for await x in store.stream("x", bufferingPolicy: .unbounded).prefix(2) {
    ///    let y = try await store.get("y")
    ///    print("🔍 (\(x), \(y))")
    ///  }
    /// }
    ///
    /// await store.transaction { store in
    ///   store.set("x", to: 1)
    ///   store.set("y", to: 2)
    ///   store.transaction { store in
    ///     store.set("x", to: 3)
    ///     store.set("y", to: 4)
    ///   }
    /// }
    ///
    /// await store.transaction { store in
    ///   store.set("x", to: 5)
    ///   store.set("y", to: 6)
    /// }
    ///
    /// try await task.value
    /// // 🔍 (3, 4)
    /// // 🔍 (5, 6)
    /// ```
    actor Store {
        public typealias TransactionLevel = UInt

        private var storedDictionary: Dictionary

        public var dictionary: Dictionary {
            get { storedDictionary }
            set {
                // A replacement also removes keys introduced by earlier staged
                // writes, even though they are not in the committed dictionary.
                var keys = Set(storedDictionary.keys).union(newValue.keys)
                for updates in transactionUpdates.values {
                    keys.formUnion(updates.map { $0.0 })
                }
                var updates = BatchUpdate()
                for key in keys { updates.set(key, to: newValue[key]) }
                batch(updates)
            }
        }

        typealias ID = UInt
        typealias Subject = [ID: AsyncStream<Value?>.Continuation]

        var count: UInt = 0
        var subscriptions: [Key: Subject] = [:]

        public private(set) var transactionLevel: TransactionLevel = 0
        public private(set) var transactionUpdates: [TransactionLevel: BatchUpdate] = [:]

        /// Initializes a new instance of `Dictionary.Store` with the specified initial dictionary value.
        ///
        /// - Parameter initialValue: The initial value to be stored in the `Dictionary.Store`.
        public init(_ initialValue: Dictionary = [:]) {
            self.storedDictionary = initialValue
        }
    }
}

public extension Dictionary.Store {
    struct BatchUpdate: Sendable, Sequence, ExpressibleByArrayLiteral {
        public typealias Element = (Key, Value?)
        private var _storage: [Element]

        public init() {
            self._storage = []
        }

        public init(arrayLiteral elements: Element...) {
            self._storage = elements
        }

        mutating func append<S>(contentsOf newElements: S) where S: Sequence, S.Element == Element {
            _storage.append(contentsOf: newElements)
        }

        public func makeIterator() -> [Element].Iterator {
            _storage.makeIterator()
        }
    }
}

public extension Dictionary.Store.BatchUpdate {
    func get(_ key: Key) throws -> Value {
        guard let value = _storage.last(where: { $0.0 == key })?.1 else {
            throw [Key: Value].Store.Error.nilAt(key: key)
        }
        return value
    }

    mutating func set(_ key: Key, to value: Value?) {
        _storage.append((key, value))
    }
}

// MARK: - Streaming

public extension Dictionary.Store {
    /// A strategy that handles exhaustion of a buffer’s capacity.
    typealias BufferingPolicy = AsyncStream<Value?>.Continuation.BufferingPolicy

    /// Creates and returns an `AsyncStream` that streams the values stored in the `ValueStore`.
    ///
    /// - Parameter bufferingPolicy: The buffering policy to be used for the stream. Default is `.bufferingNewest(1)`.
    /// - Returns: An `AsyncStream` that streams the values stored in the `ValueStore`.
    func stream(_ key: Key, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> AsyncStream<Value?> {
        AsyncStream(bufferingPolicy: bufferingPolicy) { continuation in insert(continuation, for: key) }
    }
}

private extension Dictionary.Store {
    func insert(_ continuation: AsyncStream<Value?>.Continuation, for key: Key) {
        continuation.yield(dictionary[key])
        count += 1
        let id = count
        subscriptions[key, default: [:]][id] = continuation
        continuation.onTermination = { @Sendable [weak self] _ in
            guard let self else { return }
            Task { @concurrent in await self.remove(continuation: id, for: key) }
        }
    }

    func remove(continuation id: ID, for key: Key) {
        subscriptions[key]?[id] = nil
    }
}

// MARK: - Transaction and Batching

public extension Dictionary.Store {
    /// True if the store is currently within a transaction
    var isInTransaction: Bool {
        transactionLevel > 0
    }

    /// Executes a transaction on the store allowing for multiple updates to be performed atomically.
    /// - Parameters:
    ///     - updates: A closure that takes a `Store` instance as its parameter to perform any updates.
    ///
    /// > Note: The transaction is performed atomically, meaning that all updates within the transaction are either applied together or none of them are applied.
    ///
    /// > Important: It is important to handle any errors thrown by the updates closure to ensure the transaction is properly handled. If an error is thrown during a transaction then none of the values are written to the store.
    func transaction(_ updates: @Sendable (isolated Dictionary.Store) throws -> Void) rethrows {
        transactionLevel += 1
        do {
            try updates(self)
            let levelUpdates = transactionUpdates.removeValue(forKey: transactionLevel) ?? []
            transactionLevel -= 1
            if transactionLevel > 0 {
                transactionUpdates[transactionLevel, default: []].append(contentsOf: levelUpdates)
            } else {
                batch(levelUpdates)
            }
        } catch {
            transactionUpdates.removeValue(forKey: transactionLevel)
            transactionLevel -= 1
            throw error
        }
    }

    /// Perform a batch update.
    ///
    /// This method updates the store with the key-value pairs. It also notifies any subscribed subjects of the updated values after they have all been set - this is done in order to prevent updates with intermediate states.
    ///
    /// - Complexity: O(n), where n is the number of key-value pairs in the `updates` dictionary.
    func batch(_ updates: BatchUpdate) {
        guard transactionLevel == 0 else {
            transactionUpdates[transactionLevel, default: []].append(contentsOf: updates)
            return
        }
        var array: [Key] = []
        var set: Set<Key> = []
        for (key, value) in updates {
            storedDictionary[key] = value
            if set.insert(key).inserted {
                array.append(key)
            }
        }
        for key in array {
            let value = dictionary[key]
            if let subscription = subscriptions[key] {
                for subject in subscription.values {
                    subject.yield(value)
                }
            }
        }
    }
}

// MARK: - Access

public extension Dictionary.Store {
    /// Accesses the value with the given key
    @inlinable subscript(_ key: Key) -> Value? {
        get { dictionary[key] }
        set { set(key, to: newValue) }
    }

    /// Accesses the value with the given key
    /// - Parameter key: The key to find in the dictionary.
    func get(_ key: Key) throws -> Value {
        guard let value = dictionary[key] else {
            throw Error.nilAt(key: key)
        }
        return value
    }

    /// Sets the value of the `Dictionary.Store` to the specified new value.
    ///
    /// - Parameter key: The key to find in the dictionary.
    /// - Parameter newValue: The new value to be set.
    func set(_ key: Key, to newValue: Value?) {
        guard transactionLevel == 0 else {
            return transactionUpdates[transactionLevel, default: []].set(key, to: newValue)
        }
        storedDictionary[key] = newValue
        if let subscription = subscriptions[key] {
            for subject in subscription.values {
                subject.yield(newValue)
            }
        }
    }
}

// MARK: - Error

public extension Dictionary.Store {
    enum Error: Swift.Error {
        case nilAt(key: Key)
    }
}

extension Dictionary.Store: Equatable {
    public static func == (lhs: Dictionary.Store, rhs: Dictionary.Store) -> Bool {
        lhs === rhs
    }
}
