/// Composes routed asynchronous sources without owning their subscriptions.
///
/// A Lake chooses one source before opening its sequence. The chosen sequence is
/// returned directly: buffering, replay, sharing, values (including `nil`) and
/// termination remain the source's responsibility. In particular, an empty or
/// failing source never causes a Lake to try a different mount.
///
/// `Request` can include context beyond a route, such as an entity identifier or
/// locale. Mount selection inspects a route, but sources receive the full request.
public struct Lake<Request: Sendable, Values: AsyncSequence & Sendable>: Sendable where Values.Element: Sendable {
    public typealias Source = @Sendable (Request) async throws -> Values

    private let open: Source

    /// Supplies custom routing, for example semantic type matching. The closure
    /// can delegate to a Pond, a store, or another Lake's `stream` method.
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
    struct Mount<Key: Hashable & Sendable>: Sendable {
        public let route: [Key]
        public let source: Source

        /// Matches this route and its descendants. Sources receive the original
        /// request, including the mount prefix; mounting does not rebase routes.
        public init(at route: [Key], source: @escaping Source) {
            self.route = route
            self.source = source
        }
    }

    enum RoutingError<Key: Hashable & Sendable>: Error, Equatable {
        case duplicateMount([Key])
        case unmounted([Key])
    }

    /// The deepest matching prefix wins, independently of registration order.
    /// An empty route mounts a default source. Duplicate routes are rejected.
    init<Key>(
        mounts: [Mount<Key>],
        route: @escaping @Sendable (Request) -> [Key]
    ) throws {
        var routes: Set<[Key]> = []
        for mount in mounts {
            guard routes.insert(mount.route).inserted else {
                throw RoutingError<Key>.duplicateMount(mount.route)
            }
        }
        let mounts = mounts.sorted { $0.route.count > $1.route.count }
        self.init { request in
            let path = route(request)
            guard let mount = mounts.first(where: { path.starts(with: $0.route) }) else {
                throw RoutingError<Key>.unmounted(path)
            }
            return try await mount.source(request)
        }
    }

    /// Mounts sources using the request itself as a route.
    init<Key>(mounts: [Mount<Key>]) throws where Request == [Key] {
        try self.init(mounts: mounts, route: { $0 })
    }
}
