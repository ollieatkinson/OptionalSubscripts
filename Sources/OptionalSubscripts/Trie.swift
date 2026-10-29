/// A route trie with values at any depth. Construct before sharing; a captured
/// immutable copy supports concurrent lookups without locks or global actors.
struct Trie<Key: Hashable & Sendable, Value: Sendable>: Sendable {
    private var value: Value?
    private var children: [Key: Self] = [:]

    /// Inserts a new mount, returning false if this exact route already exists.
    mutating func insert<Route: Collection>(_ route: Route, value: Value) -> Bool where Route.Element == Key {
        guard let key = route.first else {
            guard case .none = self.value else { return false }
            self.value = value
            return true
        }
        return children[key, default: Self()].insert(route.dropFirst(), value: value)
    }

    /// Finds the deepest mounted prefix in O(route depth), independent of the
    /// number or registration order of sibling routes.
    func deepestValue<Route: Sequence>(along route: Route) -> Value? where Route.Element == Key {
        var node = self
        var match = node.value
        for key in route {
            guard let child = node.children[key] else { break }
            node = child
            if let value = node.value { match = value }
        }
        return match
    }
}
