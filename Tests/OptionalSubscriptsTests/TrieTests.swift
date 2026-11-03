import OptionalSubscripts
import Testing

struct TrieTests {
    @Test func exact_paths_and_the_empty_path_address_values_and_subtrees() {
        var trie = Trie<String, Int>(value: 1)
        trie[["api", "name"]] = 2
        let root: Trie<String, Int>? = trie[[]]
        let branch: Trie<String, Int>? = trie[["api"]]
        let leaf: Int? = trie[["api", "name"]]
        let absent: Int? = trie[["api", "other"]]
        #expect(root?.value == 1)
        #expect(branch?.branches["name"]?.value == 2)
        #expect(leaf == 2)
        #expect(absent == nil)
    }

    @Test func replacing_and_removing_subtrees_preserves_other_branches() {
        var trie = Trie<String, Int>(value: 1)
        trie[["api", "old"]] = 2
        trie[["other"]] = 3
        trie[["api"]] = Trie(value: 4, branches: ["new": Trie(value: 5)])
        let old: Int? = trie[["api", "old"]]
        let new: Int? = trie[["api", "new"]]
        #expect(old == nil)
        #expect(new == 5)
        trie[["api"]] = Optional<Trie<String, Int>>.none
        #expect(trie.branches["api"] == nil)
        #expect(trie.branches["other"]?.value == 3)
        trie[["missing", "child"]] = Optional<Trie<String, Int>>.none
        #expect(trie.branches["missing"] == nil)
        trie[[]] = Optional<Trie<String, Int>>.none
        #expect(trie.value == nil && trie.branches.isEmpty)
    }

    @Test func clearing_a_value_keeps_its_descendants() {
        var trie = Trie<String, Int>()
        trie[["api"]] = 1
        trie[["api", "child"]] = 2
        trie[["api"]] = Optional<Int>.none
        let branch: Int? = trie[["api"]]
        let child: Int? = trie[["api", "child"]]
        #expect(branch == nil)
        #expect(child == 2)
    }

    @Test func insertion_is_lazy_and_supports_mutating_the_stored_value() {
        var evaluations = 0
        func initial() -> [Int] { evaluations += 1; return [1] }
        var trie = Trie<String, [Int]>()
        trie[["api"], inserting: initial()].append(2)
        trie[["api"], inserting: initial()].append(3)
        let values: [Int]? = trie[["api"]]
        #expect(values == [1, 2, 3])
        #expect(evaluations == 1)
    }

    @Test func depth_first_search_honours_sorted_and_insertion_order() {
        var trie = Trie<String, Int>(value: 0)
        trie[["z", "child"]] = 1
        trie[["a"]] = 2
        #expect(trie.depthFirstSearch().map(\.path) == [[], ["a"], ["z"], ["z", "child"]])
        #expect(trie.depthFirstSearch(sorted: false).map(\.path) == [[], ["z"], ["z", "child"], ["a"]])
        #expect(trie.depthFirstSearch(sorted: false).map(\.value) == [0, nil, 1, 2])
    }

    @Test func longest_prefix_preserves_present_optional_nil_and_root_fallback() {
        var trie = Trie<String, Int?>(value: .some(1))
        trie[["api"]] = Optional<Int?>.some(nil)
        trie[["api", "details"]] = Optional<Int?>.some(2)
        let nilMatch = trie.deepestValue(along: ["api", "other"])
        #expect(nilMatch != nil)
        #expect(nilMatch.flatMap { $0 } == nil)
        #expect(trie.deepestValue(along: ["api", "details", "child"]) == .some(2))
        #expect(trie.deepestValue(along: ["apiary"]) == .some(1))
    }

    @Test func a_shared_snapshot_is_unaffected_by_later_mutation() async {
        var trie = Trie<String, Int>()
        trie[["api", "name"]] = 1
        let snapshot = trie
        trie[["api", "name"]] = 2
        let read = Task { @concurrent in snapshot.deepestValue(along: ["api", "name"]) }
        #expect(await read.value == 1)
        #expect(trie.deepestValue(along: ["api", "name"]) == 2)
    }
}
