import Foundation
@testable import OptionalSubscripts
import Testing
#if canImport(Combine)
import Combine
#endif

struct OptionalTests {
    @Test func dynamic_subscripts_keep_typed_reads_holes_key_paths_and_pruning() throws {
        var value: Any? = [:]
        value[] = "hello"
        #expect(try value[] as String == "hello")
        value["one"] = 1
        #expect(try value["one"] as Int == 1)
        value["one", 2] = 2
        value["one", 3] = nil
        #expect(try value["one"] as [Int?] == [nil, nil, 2])
        value["one", 2] = ["three": 4]
        #expect(try value["one", 2, "three"] as Int == 4)
        #expect(try value[\.["one"][2]["three"]] as Int == 4)
        value["one", 2] = nil
        #expect(value["one"] == nil)
        value["one", "two"] = nil
        #expect(value[] == nil)
        value[-1] = "ignored"
        #expect(value == nil)
    }

    @Test func sendable_routes_distinguish_keys_indices_absence_null_and_custom_scalars() throws {
        struct Token: Equatable, Sendable { let id: Int }
        var value: SendableValue = nil
        value["items", 2, "value"] = Token(id: 7)
        #expect(try value["items", 2, "value"].cast(to: Token.self) == Token(id: 7))
        #expect(value["items", "2", "value"] == nil)
        value["items", 2, "value"] = NSNull()
        #expect(value["items", 2, "value"] is NSNull)
        value["items", 2, "value"] = nil
        #expect(value == nil)
        value[-1] = "ignored"
        #expect(value == nil)
        let converted = try SendableValue(jsonObject: ["a": [NSNull(), "b"]])
        #expect(converted["a", 0] is NSNull)
        #expect(converted["a", 1] as? String == "b")
        #expect(try converted.json(.sortedKeys).string() == "{\"a\":[null,\"b\"]}")
        #expect(throws: JSONConversionError.self) { try SendableValue(jsonObject: Date()) }
    }

    @Test func locations_and_routes_keep_original_literal_ordering() {
        let index: Any?.Location = 5
        let key: Any?.Location = "5"
        #expect(index != key)
        #expect(index < key)
        #expect(Any?.Location.index(4) < index)
        #expect(Any?.Location.key("4") < key)
        let routes: [Any?.Route] = [["b"], ["a", 2], [], ["a"], [2]]
        #expect(routes.sorted(by: <) == [[], [2], ["a"], ["a", 2], ["b"]])
    }

    @Test func tree_distinguishes_value_removal_from_subtree_replacement_and_traverses() {
        var tree = Trie<Any?.Location, Int>()
        tree[[]] = 0
        tree[[1]] = 1
        tree[[1, 2, 3]] = 3
        tree[[1, "2", 3]] = 4
        tree[[1]] = Int?.none
        #expect(tree[[1, 2, 3]] as Int? == 3)
        #expect(tree[[1, "2", 3]] as Int? == 4)
        tree[[1]] = Trie?.none
        #expect(tree[[1, 2, 3]] as Int? == nil)
        tree[[1, 2]] = Trie(value: nil, branches: ["a": Trie(value: 3), "b": Trie(value: 5)])
        var visited: [Any?.Route: Int] = [:]
        for (route, value) in tree.depthFirstSearch() { if let value { visited[route] = value } }
        #expect(visited == [Any?.Route(): 0, [1, 2, "a"]: 3, [1, 2, "b"]: 5])
    }

    @Test func seeded_route_ordering_and_complete_tree_traversal_match_original_scenarios() {
        #if canImport(GameplayKit)
        let generated = Any?.RandomRoutes(keys: "abcde".map(String.init), indices: Array(1...3),
                                           keyBias: 0.8, length: 0...5, seed: 4).generate(count: 10)
        #expect(generated.sorted(by: <) == [[], [3, "a"], [3, "a", "d", "e"], ["a"],
            ["a", 2, "d"], ["a", "e", 3, "b"], ["b", "e", "e"],
            ["c", 3, "d", "e", "d"], ["d"], ["d", 3]])
        #endif
        var tree = Trie<Int, Int>()
        var expected: [[Int]: Int] = [:]
        for x in 1...5 { for y in 1...5 { for z in 1...5 {
            tree[[x, y, z]] = x * y * z
            expected[[x, y, z]] = x * y * z
        } } }
        for (route, value) in tree.depthFirstSearch() { if expected[route] == value { expected.removeValue(forKey: route) } }
        #expect(expected.isEmpty)
    }

    @Test func cast_failures_keep_safe_diagnostics_and_json_rejects_invalid_objects() throws {
        let value: Any? = 3
        #expect(throws: CastingError.self) { try value.cast(to: String.self) }
        #expect(throws: Any?.JSONSerializationError.self) { try Any?.none.json() }
        #expect(throws: Any?.JSONSerializationError.self) { try Any?.some(Date()).json() }
        let path: Any?.Route = (0..<12).map { .key("k\($0)") }
        var document: Any? = nil
        document[path] = "deep"
        #expect(try document[Any?.keyPath(path)] as String == "deep")
    }

    #if canImport(Combine)
    @Test func synchronous_combine_cast_and_filter_operators_remain_available() {
        let values = Just<Any>("hello").filter(String.self)
        let token = values.sink { #expect($0 == "hello") }
        token.cancel()
    }
    #endif
}
