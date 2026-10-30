import OrderedCollections

/// A path-indexed tree, with optional values at every node and insertion-ordered
/// branches. Mutate while building; an immutable copy can be shared when its keys
/// and values are Sendable. Adapted from Oliver Atkinson's Trie:
/// https://gist.github.com/ollieatkinson/6c11013dca11d6570112ef08f70f5a8b
public struct Trie<Key: Hashable, Value> {
    public var value: Value?
    public var branches: OrderedDictionary<Key, Self>

    public init(value: Value? = nil, branches: OrderedDictionary<Key, Self> = [:]) {
        self.value = value
        self.branches = branches
    }
}

extension Trie: Sendable where Key: Sendable, Value: Sendable {}

public extension Trie {
    /// Reads or replaces an entire subtree. The empty path addresses this node.
    /// Assigning nil removes the selected branch; at the root it clears the trie.
    subscript(path: some Collection<Key>) -> Self? {
        get {
            var node = self
            for key in path {
                guard let child = node.branches[key] else { return nil }
                node = child
            }
            return node
        }
        set {
            guard let key = path.first else {
                self = newValue ?? Self()
                return
            }
            let remainder = path.dropFirst()
            if remainder.isEmpty {
                branches[key] = newValue
            } else if newValue != nil || branches[key] != nil {
                branches[key, default: Self()][remainder] = newValue
            }
        }
    }

    /// Reads or replaces the exact path's value, preserving its descendants.
    /// A missing value is different from a Value that is itself an optional nil.
    @_disfavoredOverload
    subscript(path: some Collection<Key>) -> Value? {
        get {
            let node: Self? = self[path]
            return node?.value
        }
        set {
            guard let key = path.first else {
                value = newValue
                return
            }
            if newValue != nil || branches[key] != nil {
                branches[key, default: Self()][path.dropFirst()] = newValue
            }
        }
    }

    /// Inserts a value only when absent. The default is evaluated only on a miss.
    subscript(path: some Collection<Key>, inserting defaultValue: @autoclosure () -> Value) -> Value {
        mutating get {
            let node: Self? = self[path]
            if let value = node?.value { return value }
            let value = defaultValue()
            self[path] = value
            return value
        }
        set { self[path] = newValue }
    }

    /// Finds the deepest valued prefix in O(path depth), independent of the
    /// number or registration order of sibling branches. The root is a fallback.
    func deepestValue(along path: some Sequence<Key>) -> Value? {
        var node = self
        var match = node.value
        for key in path {
            guard let child = node.branches[key] else { break }
            node = child
            if let value = node.value { match = value }
        }
        return match
    }
}

extension Trie {
    /// Lake rejects duplicate mounts rather than replacing an existing value.
    mutating func insert(_ path: some Collection<Key>, value: Value) -> Bool {
        guard let key = path.first else {
            guard case .none = self.value else { return false }
            self.value = value
            return true
        }
        return branches[key, default: Self()].insert(path.dropFirst(), value: value)
    }
}

public extension Trie where Key: Comparable {
    /// Visits this node, then each complete subtree. Sorted traversal orders
    /// siblings by key; otherwise their insertion order is preserved.
    func depthFirstSearch(sorted: Bool = true) -> some Sequence<(path: [Key], value: Value?)> {
        DepthFirstSearch(trie: self, sorted: sorted)
    }

    private struct DepthFirstSearch: IteratorProtocol, Sequence {
        private var stack: [(path: [Key], node: Trie)]
        private let sorted: Bool

        init(trie: Trie, sorted: Bool) {
            stack = [(path: [], node: trie)]
            self.sorted = sorted
        }

        mutating func next() -> (path: [Key], value: Value?)? {
            guard let (path, node) = stack.popLast() else { return nil }
            let keys = sorted ? node.branches.keys.sorted() : Array(node.branches.keys)
            for key in keys.reversed() {
                if let subtree = node.branches[key] {
                    stack.append((path: path + [key], node: subtree))
                }
            }
            return (path, node.value)
        }

        func makeIterator() -> Self { self }
    }
}

// Store notifications include ancestors and all observed descendants of a write.
extension Trie where Key: Comparable {
    func routes<Route: Collection>(affectedByChanging route: Route) -> [[Key]] where Route.Element == Key {
        var paths: [[Key]] = []
        var prefix: [Key] = []
        var node = self
        for key in route {
            if node.value != nil { paths.append(prefix) }
            guard let child = node.branches[key] else { return paths }
            prefix.append(key)
            node = child
        }
        for (suffix, value) in node.depthFirstSearch(sorted: false) where value != nil {
            paths.append(prefix + suffix)
        }
        return paths
    }
}
