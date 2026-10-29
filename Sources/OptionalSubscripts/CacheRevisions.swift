import Synchronization

/// Owned by one Pond, never retained by Store. Closing rejects queued writes;
/// a new observation generation uses a fresh tracker instead of reviving it.
@available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
final class CacheRevisions: Sendable {
    private struct State {
        var closed = false
        var routes: [Any?.Route: UInt64] = [:]
    }
    private let state = Mutex(State())

    func invalidate(_ routes: [Any?.Route], revision: UInt64) {
        state.withLock { state in
            guard !state.closed else { return }
            for route in routes { state.routes[route] = max(revision, state.routes[route] ?? 0) }
        }
    }

    func close() { state.withLock { $0.closed = true; $0.routes.removeAll() } }

    func commit(_ updates: Any?.Store.BatchUpdates, revision: UInt64, body: (Any?.Store.BatchUpdates) -> Void) {
        state.withLock { state in
            guard !state.closed else { return }
            let fresh = updates.filter { revision >= (state.routes[$0.0] ?? 0) }
            for (route, _) in fresh { state.routes[route] = revision }
            // Invalidation and a non-suspending Store commit share one critical
            // section, so a queued old write cannot race past a remap or close.
            body(fresh)
        }
    }
}
