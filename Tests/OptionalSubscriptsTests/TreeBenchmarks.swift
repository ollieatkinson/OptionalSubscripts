import Foundation
@testable import OptionalSubscripts
import Testing

#if canImport(GameplayKit)
@Suite(.serialized, .enabled(if: ProcessInfo.processInfo.environment["TREE_BENCHMARK"] == "1"))
struct TreeBenchmarks {
    @Test func compare_tree_and_trie() {
        let original = Any?.RandomRoutes(keys: "abcde".map(String.init), indices: Array(1...3),
            keyBias: 0.8, length: 5...20, seed: 4).generate(count: 10_000)
        let wide: [Any?.Route] = (0..<10_000).map { [.index($0)] }
        for (name, routes) in [("original_10000", original), ("wide_10000", wide)] {
            compare(name, "build", tree: { buildTree(routes) }, trie: { buildTrie(routes) },
                consumeTree: { readTree($0, routes) }, consumeTrie: { readTrie($0, routes) })
            let tree = buildTree(routes)
            let trie = buildTrie(routes)
            compare(name, "lookup", tree: { readTree(tree, routes) }, trie: { readTrie(trie, routes) },
                consumeTree: { $0 }, consumeTrie: { $0 })
            compare(name, "update_snapshot", tree: {
                var copy = tree
                for (index, route) in routes.enumerated() { copy[route] = index + 1 }
                return copy
            }, trie: {
                var copy = trie
                for (index, route) in routes.enumerated() { copy[route] = index + 1 }
                return copy
            }, consumeTree: { readTree($0, routes) }, consumeTrie: { readTrie($0, routes) })
            for sorted in [false, true] {
                compare(name, sorted ? "traverse_sorted" : "traverse_unsorted", tree: {
                    var sum = 0
                    tree.traverse(sorted: sorted) { sum += ($0.value ?? 0) + $0.route.count }
                    return sum
                }, trie: {
                    trie.depthFirstSearch(sorted: sorted).reduce(0) { $0 + ($1.value ?? 0) + $1.path.count }
                }, consumeTree: { $0 }, consumeTrie: { $0 })
            }
        }
    }
}

private typealias Route = Any?.Route
private typealias OldTree = Tree<Any?.Location, Int>
private typealias NewTrie = Trie<Any?.Location, Int>

@inline(never) private func buildTree(_ routes: [Route]) -> OldTree {
    var tree = OldTree()
    for (index, route) in routes.enumerated() { tree[route] = index }
    return tree
}

@inline(never) private func buildTrie(_ routes: [Route]) -> NewTrie {
    var trie = NewTrie()
    for (index, route) in routes.enumerated() { trie[route] = index }
    return trie
}

@inline(never) private func readTree(_ tree: OldTree, _ routes: [Route]) -> Int {
    routes.reduce(0) { sum, route in
        let value: Int? = tree[route]
        return sum + (value ?? 0)
    }
}

@inline(never) private func readTrie(_ trie: NewTrie, _ routes: [Route]) -> Int {
    routes.reduce(0) { sum, route in
        let value: Int? = trie[route]
        return sum + (value ?? 0)
    }
}

// Consume results after timing; alternate execution order and warm both variants.
private func compare<A, B>(_ dataset: String, _ operation: String,
    tree: () -> A, trie: () -> B, consumeTree: (A) -> Int, consumeTrie: (B) -> Int) {
    let expected = consumeTree(tree())
    precondition(consumeTrie(trie()) == expected)
    var oldTimes: [Double] = []
    var newTimes: [Double] = []
    for round in 0..<9 {
        if round.isMultiple(of: 2) {
            oldTimes.append(sample(tree, consume: consumeTree, expected: expected))
            newTimes.append(sample(trie, consume: consumeTrie, expected: expected))
        } else {
            newTimes.append(sample(trie, consume: consumeTrie, expected: expected))
            oldTimes.append(sample(tree, consume: consumeTree, expected: expected))
        }
    }
    oldTimes.sort(); newTimes.sort()
    let old = oldTimes[oldTimes.count / 2]
    let new = newTimes[newTimes.count / 2]
    print("TREE_BENCHMARK dataset=\(dataset) operation=\(operation) samples=9 tree_ms=\(old) trie_ms=\(new) trie_over_tree=\(new / old) tree_range_ms=\(oldTimes.first!)...\(oldTimes.last!) trie_range_ms=\(newTimes.first!)...\(newTimes.last!)")
}

@inline(never) private func sample<T>(_ operation: () -> T, consume: (T) -> Int, expected: Int) -> Double {
    let start = ContinuousClock.now
    let value = operation()
    let elapsed = start.duration(to: .now).components
    precondition(consume(value) == expected)
    return Double(elapsed.seconds) * 1_000 + Double(elapsed.attoseconds) / 1e15
}
#endif
