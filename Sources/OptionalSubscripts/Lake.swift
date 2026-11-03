/// Composes routed asynchronous sources without owning their subscriptions.
///
/// A Lake chooses one source before opening its sequence. The chosen sequence is
/// returned directly: buffering, replay, sharing, values (including `nil`) and
/// termination remain the source's responsibility. In particular, an empty or
/// failing source never causes a Lake to try a different inlet.
///
/// `Request` can include context beyond a route, such as an entity identifier or
/// locale. Inlet selection inspects a route, but sources receive the full request.
public struct Lake<Request: Sendable, Values: AsyncSequence & Sendable>: Sendable where Values.Element: Sendable {
    public typealias Source = @Sendable (Request) async throws -> Values

    private let open: Source

    /// Supplies custom routing, for example semantic type matching. The closure
    /// can delegate to a ValuePond, a store, or another Lake's `stream` method.
    public init(stream: @escaping Source) {
        self.open = stream
    }

    /// Opens the selected source in the calling task, without a forwarding task
    /// or an additional stream buffer. Cancellation of iteration reaches the
    /// original sequence directly.
    public func stream(_ request: Request) async throws -> Values {
        try Task.checkCancellation()
        let values = try await open(request)
        try Task.checkCancellation()
        return values
    }
}

public extension Lake {
    struct Inlet<Key: Hashable & Sendable>: Sendable {
        public let route: [Key]
        public let source: Source

        /// Matches this route and its descendants. Sources receive the original
        /// request, including the inlet prefix; connecting does not rebase routes.
        public init(at route: [Key], source: @escaping Source) {
            self.route = route
            self.source = source
        }
    }

    enum RoutingError<Key: Hashable & Sendable>: Error, Equatable {
        case duplicateInlet([Key])
        case noSource([Key])
    }

    /// The deepest matching prefix wins, independently of registration order.
    /// An empty route connects a default source. Duplicate routes are rejected.
    /// An immutable trie is built once; selection takes O(route depth).
    init<Key>(
        inlets: [Inlet<Key>],
        route: @escaping @Sendable (Request) -> [Key]
    ) throws {
        var trie = Trie<Key, Source>()
        for inlet in inlets {
            guard trie.insert(inlet.route, value: inlet.source) else {
                throw RoutingError<Key>.duplicateInlet(inlet.route)
            }
        }
        let sources = trie
        self.init { request in
            let path = route(request)
            guard let source = sources.deepestValue(along: path) else {
                throw RoutingError<Key>.noSource(path)
            }
            return try await source(request)
        }
    }

    /// Connects sources using the request itself as a route.
    init<Key>(inlets: [Inlet<Key>]) throws where Request == [Key] {
        try self.init(inlets: inlets, route: { $0 })
    }
}
