//
// github.com/screensailor 2021
//

public extension Optional where Wrapped == Any {
    
    actor Store {
        
        public typealias BatchUpdates = [(Route, SendableValue)]
        public typealias TransactionLevel = UInt
        
        public var data: SendableValue
        
        public private(set) var transactionLevel: TransactionLevel = 0
        public private(set) var transactionUpdates: [TransactionLevel: BatchUpdates] = [:]
        
        typealias ID = UInt
        typealias Subject = [ID: AsyncStream<SendableValue>.Continuation]
        
        var count: UInt = 0
        var subscriptions = Trie<Location, Subject>()
        
        public init(_ data: SendableValue = nil) {
            self.data = data
        }
    }
}

public extension Optional.Store where Wrapped == Any {
    
    typealias BufferingPolicy = AsyncStream<SendableValue>.Continuation.BufferingPolicy
    
    @inlinable func stream(_ route: Location..., bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> AsyncStream<SendableValue> {
        stream(route, bufferingPolicy: bufferingPolicy)
    }
    
    func stream<Route>(_ route: Route, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> AsyncStream<SendableValue> where Route: Collection, Route.Index == Int, Route.Element == Location {
        let route = Array(route)
        return AsyncStream(bufferingPolicy: bufferingPolicy) { continuation in
            self.insert(continuation, for: route)
        }
    }
}

private extension Optional.Store where Wrapped == Any {
    
    func insert<Route>(_ continuation: AsyncStream<SendableValue>.Continuation, for route: Route) where Route: Collection, Route.Index == Int, Route.Element == Location {
        let route = Array(route)
        continuation.yield(data[route])
        let id = count + 1
        count = id
        subscriptions[route, inserting: Subject()][id] = continuation
        continuation.onTermination = { @Sendable [weak self] termination in
            guard let self = self else { return }
            Task { @concurrent in
                await self.remove(continuation: id, for: route)
            }
        }
    }
    
    func remove<Route>(continuation id: ID, for route: Route) where Route: Collection, Route.Index == Int, Route.Element == Location {
        subscriptions[route]?[id] = nil
        if subscriptions[route]?.isEmpty == true {
            subscriptions[route] = Subject?.none
        }
    }
}

public extension Optional.Store where Wrapped == Any {
    
    var isInTransaction: Bool {
        transactionLevel > 0
    }
    
    func transaction(_ updates: @Sendable (isolated Optional<Any>.Store) throws -> Void) rethrows {
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
        }
        catch {
            transactionUpdates.removeValue(forKey: transactionLevel)
            transactionLevel -= 1
            throw error
        }
    }
    
    func batch(_ updates: BatchUpdates) {
        var routes: Set<Route> = []
        for (route, value) in updates {
            data[route] = value
            routes.formUnion(
                subscriptions
                    .routes(affectedByChanging: route)
                    .filter{ subscriptions[$0]?.value != nil }
            )
        }
        for route in routes.sorted(by: <) {
            guard let subject: Subject = subscriptions[route] else { continue }
            let value = data[route]
            subject.values.forEach { $0.yield(value) }
        }
    }
}

public extension Optional.Store where Wrapped == Any {
    
    @inlinable func set(_ route: Location..., to value: SendableValue) {
        set(route, to: value)
    }
    
    func set<Route>(_ route: Route, to value: SendableValue) where Route: Collection, Route.Index == Int, Route.Element == Location {
        guard transactionLevel == 0 else {
            return transactionUpdates[transactionLevel, default: []].append((Array(route), value))
        }
        data[route] = value
        didSet(route, to: value)
    }
}

extension Optional.Store where Wrapped == Any {
    
    func didSet<Route>(_ route: Route, to value: SendableValue) where Route: Collection, Route.Index == Int, Route.Element == Location {
        for route in route.lineage.reversed() {
            guard let subject: Subject = subscriptions[route] else {
                continue
            }
            let data = data[route]
            subject.values.forEach { $0.yield(data) }
        }
        let subtree: Trie<Location, Subject>? = subscriptions[route]
        for (subroute, subject) in subtree?.depthFirstSearch() ?? Trie<Location, Subject>().depthFirstSearch() {
            guard let subject else { continue }
            let data = value[subroute]
            subject.values.forEach { $0.yield(data) }
        }
    }
}

public extension Optional.Store where Wrapped == Any {
    
    typealias Route = Optional<Any>.Route
    typealias Location = Optional<Any>.Location
    typealias KeyPath = Optional<Any>.KeyPath
    
    @inlinable subscript<A: Sendable>(_ route: Location..., as type: A.Type = A.self) -> A {
        get throws {
            try get(route)
        }
    }
    
    @inlinable subscript<A: Sendable, Route>(_ route: Route, as a: A.Type = A.self) -> A where Route: Collection, Route.Index == Int, Route.Element == Location {
        get throws {
            try get(route)
        }
    }
    
    @inlinable func get<A: Sendable>(_ route: Location..., as type: A.Type = A.self) throws -> A {
        try get(route)
    }
    
    func get<A: Sendable, Route>(_ route: Route, as a: A.Type = A.self) throws -> A where Route: Collection, Route.Index == Int, Route.Element == Location {
        guard let any = data[route] else {
            throw Error.nilAt(route: Array(route))
        }
        guard let a = try? Optional(any).cast(to: A.self) else {
            throw Error.casting(route: Array(route), from: type(of: any), to: A.self)
        }
        return a
    }
}

public extension Optional.Store where Wrapped == Any {
    
    enum Error: Swift.Error {
        case nilAt(route: Route)
        case casting(route: Route, from: Any.Type, to: Any.Type)
    }
}

// Validation and commit cannot suspend between ownership checks and mutation.
@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
extension Optional.Store where Wrapped == Any {
    func cache(_ updates: BatchUpdates, revisions: CacheRevisions, revision: UInt64) {
        revisions.commit(updates, revision: revision) { batch($0) }
    }
}
