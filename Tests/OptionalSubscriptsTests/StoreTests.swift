import Foundation
@testable import OptionalSubscripts
import Testing

@Suite(.timeLimit(.minutes(1))) struct StoreTests {
    @Test func cancelling_dictionary_observers_releases_their_keys() async throws {
        let store = [String: Int].Store()
        var observers: [Task<Void, Never>] = []
        for key in 0..<100 {
            let stream = await store.stream(String(key))
            observers.append(Task { for await _ in stream {} })
        }
        #expect(await store.subscriptions.count == 100)
        for task in observers { task.cancel() }
        for task in observers { await task.value }
        while !(await store.subscriptions.values.allSatisfy(\.isEmpty)) {
            try Task.checkCancellation()
            await Task.yield()
        }
        #expect(await store.subscriptions.isEmpty)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func newer_child_writes_reject_an_older_parent_without_losing_disjoint_updates() async throws {
        let store = Any?.Store()
        let revisions = CacheRevisions()
        await store.cache([(["doc", "child"], "new")], revisions: revisions, revision: 2)
        await store.cache([(["doc"], ["child": "old"]), (["other"], "safe")], revisions: revisions, revision: 1)
        #expect(try await store.get("doc", "child", as: String.self) == "new")
        #expect(try await store.get("other", as: String.self) == "safe")
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func newer_parent_writes_reject_an_older_child() async throws {
        let store = Any?.Store()
        let revisions = CacheRevisions()
        await store.cache([(["doc"], ["child": "new"])], revisions: revisions, revision: 2)
        await store.cache([(["doc", "child"], "old")], revisions: revisions, revision: 1)
        #expect(try await store.get("doc", "child", as: String.self) == "new")
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test(arguments: [([String](), ["doc"]), (["doc"], []), (["doc"], ["doc", "child"]), (["doc", "child"], ["doc"])])
    func invalidations_reject_overlapping_routes(routes: ([String], [String])) {
        let revisions = CacheRevisions()
        revisions.invalidate([routes.0.map(Any?.Location.key)], revision: 2)
        var committed = false
        revisions.commit([(routes.1.map(Any?.Location.key), "old")], revision: 1) { committed = !$0.isEmpty }
        #expect(!committed)
    }

    @Test func typed_value_store_replays_mutates_notifies_and_compares_identity() async {
        let store = ValueStore("pizza")
        var values = await store.stream().makeAsyncIterator()
        #expect(await values.next() == "pizza")
        await store.set(to: "burger")
        #expect(await values.next() == "burger")
        await store.inout { $0 += "!" }
        #expect(await values.next() == "burger!")
        #expect(await store.value == "burger!")
        #expect(store == store)
        #expect(store != ValueStore("burger!"))
    }

    @Test func dictionary_streams_updates_and_batches_only_the_final_value() async throws {
        let store = [String: Int].Store()
        var values = await store.stream("x", bufferingPolicy: .unbounded).makeAsyncIterator()
        #expect(await values.next() == .some(nil))
        var batch = [String: Int].Store.BatchUpdate()
        for value in 1...3 { batch.set("x", to: value); batch.set("y", to: value) }
        #expect(try batch.get("x") == 3)
        await store.batch(batch)
        #expect(await values.next() == 3)
        #expect(try await store.get("y") == 3)
        #expect(await store.dictionary == ["x": 3, "y": 3])
        #expect(store == store)
        #expect(store != [String: Int].Store(["x": 3, "y": 3]))
        await #expect(throws: [String: Int].Store.Error.self) { try await store.get("missing") }
    }

    @Test func dictionary_nested_transactions_preserve_levels_and_rollback() async throws {
        let store = [String: Int].Store()
        var values = await store.stream("x", bufferingPolicy: .unbounded).makeAsyncIterator()
        #expect(await values.next() == .some(nil))
        await store.transaction { store in
            #expect(store.transactionLevel == 1)
            store.set("x", to: 1)
            store.transaction { store in
                #expect(store.transactionLevel == 2)
                store.set("x", to: 2)
                do {
                    try store.transaction { store in
                        #expect(store.transactionLevel == 3)
                        store.set("z", to: 99)
                        throw TestFailure.rollback
                    }
                } catch {}
                #expect(store.transactionLevel == 2)
                store.set("x", to: 3)
                store.set("y", to: 3)
            }
        }
        #expect(await values.next() == 3)
        #expect(await store.dictionary == ["x": 3, "y": 3])
        #expect(await store.transactionLevel == 0)
    }

    @Test func routed_streams_observe_upstream_and_downstream_changes() async throws {
        let store = Any?.Store()
        await store.set("a", 2, "c", to: "?")
        var field = await store.stream("a", 2, "c").makeAsyncIterator()
        var document = await store.stream("a", 2).makeAsyncIterator()
        #expect(try await field.next()?.cast(to: String.self) == "?")
        #expect(try await document.next()?.cast(to: [String: String].self) == ["c": "?"])
        for heart in ["red", "yellow", "green"] {
            await store.set("a", 2, to: ["c": heart])
            #expect(try await field.next()?.cast(to: String.self) == heart)
            #expect(try await document.next()?.cast(to: [String: String].self) == ["c": heart])
        }
        await store.set("a", 2, "c", to: "white")
        #expect(try await field.next()?.cast(to: String.self) == "white")
        #expect(try await document.next()?.cast(to: [String: String].self) == ["c": "white"])
    }

    @Test func routed_transactions_and_batches_publish_once_and_rollback_nested_writes() async throws {
        let store = Any?.Store()
        var values = await store.stream("x", bufferingPolicy: .unbounded).makeAsyncIterator()
        #expect(await values.next() as? Int == nil)
        await store.transaction { store in
            #expect(store.transactionLevel == 1)
            store.set("x", to: 1)
            store.transaction { store in
                #expect(store.transactionLevel == 2)
                store.set("x", to: 2)
                do {
                    try store.transaction { store in
                        #expect(store.transactionLevel == 3)
                        store.set("z", to: 99)
                        throw TestFailure.rollback
                    }
                } catch {}
                store.set("x", to: 3)
                store.set("y", to: 3)
            }
        }
        #expect(await values.next() as? Int == 3)
        #expect(try await store.get("x", as: Int.self) == 3)
        #expect(try await store.data.json(.sortedKeys).string() == "{\"x\":3,\"y\":3}")
        await #expect(throws: Any?.Store.Error.self) { try await store.get("z", as: Int.self) }
        await #expect(throws: Any?.Store.Error.self) { try await store.get("x", as: String.self) }
    }

    @Test func deep_edits_and_root_replacements_publish_one_coherent_snapshot() async throws {
        let initial: [String: any Sendable] = ["profile": ["name": "Old"], "other": true]
        let store = Any?.Store(initial)
        let root = await store.stream([], bufferingPolicy: .unbounded)
        let history = Task { () throws -> [String] in
            var snapshots: [String] = []
            for await value in root {
                if value as? String == "finished" { break }
                snapshots.append(try value.json(.sortedKeys).string())
            }
            return snapshots
        }
        await store.set("profile", "name", to: "New")
        await store.batch([(["profile", "name"], "Final"), (["other"], false)])
        await store.transaction { store in store.data = ["replacement": true] }
        await store.set([], to: ["direct": true])
        await store.set([], to: "finished")
        #expect(try await history.value == [
            "{\"other\":true,\"profile\":{\"name\":\"Old\"}}",
            "{\"other\":true,\"profile\":{\"name\":\"New\"}}",
            "{\"other\":false,\"profile\":{\"name\":\"Final\"}}",
            "{\"replacement\":true}",
            "{\"direct\":true}",
        ])
    }

    @Test func routed_batches_and_root_replacements_obey_transaction_rollback() async throws {
        let store = Any?.Store(["profile": ["name": "Original"]])
        var values = await store.stream("profile", "name", bufferingPolicy: .unbounded).makeAsyncIterator()
        #expect(await values.next() as? String == "Original")
        await store.transaction { store in
            store.batch([(["profile", "name"], "First")])
            do {
                try store.transaction { store in
                    store.data = ["profile": ["name": "Rolled back"]]
                    store.batch([(["lost"], true)])
                    throw TestFailure.rollback
                }
            } catch {}
            store.set("profile", "name", to: "Final")
        }
        #expect(await values.next() as? String == "Final")
        do {
            try await store.transaction { store in
                store.batch([(["profile", "name"], "Discarded")])
                store.data = ["lost": true]
                throw TestFailure.rollback
            }
        } catch {}
        #expect(try await store.get("profile", "name", as: String.self) == "Final")
        await #expect(throws: Any?.Store.Error.self) { try await store.get("lost", as: Bool.self) }
        await store.set("profile", "name", to: "After rollback")
        #expect(await values.next() as? String == "After rollback")
    }

    @Test func dictionary_batches_obey_enclosing_transactions() async throws {
        let store = [String: Int].Store(["x": 0])
        var values = await store.stream("x", bufferingPolicy: .unbounded).makeAsyncIterator()
        #expect(await values.next() == 0)
        await store.transaction { store in
            store.batch([("x", 1), ("discarded", 99)])
            store.dictionary = ["x": 2]
        }
        #expect(await values.next() == 2)
        await #expect(throws: [String: Int].Store.Error.self) { try await store.get("discarded") }
        do {
            try await store.transaction { store in
                store.batch([("x", 99)])
                store.dictionary = ["discarded": 99]
                throw TestFailure.rollback
            }
        } catch {}
        #expect(try await store.get("x") == 2)
        await store.set("x", to: 3)
        #expect(await values.next() == 3)
    }

    @Test func routed_subscriptions_and_concurrent_batches_produce_the_same_document() async throws {
        let original = Any?.Store()
        let copy = Any?.Store()
        let routes: [Any?.Route] = (0..<200).map { ["items", .index($0), "value"] }
        let observations = await withTaskGroup(of: AsyncObservation.self, returning: [AsyncObservation].self) { group in
            for route in routes {
                group.addTask {
                    await original.stream(route).filter(String.self).prefix(1).sink { value in
                        await copy.set(route, to: value)
                    }
                }
            }
            return await group.reduce(into: []) { $0.append($1) }
        }
        await withTaskGroup(of: Void.self) { group in
            for route in routes { group.addTask { await original.set(route, to: "value") } }
        }
        for observation in observations { await observation.wait() }
        #expect(try await original.data.json(.sortedKeys) == copy.data.json(.sortedKeys))
        let batched = Any?.Store()
        await batched.batch(routes.map { ($0, "value") })
        #expect(try await original.data.json(.sortedKeys) == batched.data.json(.sortedKeys))
    }

    @Test func task_owned_sink_preserves_observation_and_cancellation() async {
        let store = [String: Int].Store()
        let received = ValueStore([Int?]())
        let stream = await store.stream("x", bufferingPolicy: .unbounded)
        let observation = stream.prefix(3).sink { value in await received.inout { $0.append(value) } }
        await store.set("x", to: 1)
        await store.set("x", to: 2)
        await observation.wait()
        #expect(await received.value == [nil, 1, 2])
        let pending = await store.stream("x").sink { _ in }
        pending.cancel()
        await pending.wait()
    }

    @Test func routed_sink_observes_live_field_updates() async throws {
        let store = Any?.Store()
        let received = ValueStore([String]())
        let observation = await store.stream("heart", bufferingPolicy: .unbounded).filter(String.self).prefix(3).sink { value in
            await received.inout { $0.append(value) }
        }
        await store.batch([(["heart"], "red")])
        await store.set("heart", to: "yellow")
        await store.set("heart", to: "green")
        await observation.wait()
        #expect(await received.value == ["red", "yellow", "green"])
    }

    @Test func mixed_parent_child_batches_match_synchronous_optional_subscript_semantics() async throws {
        let changes: Any?.Store.BatchUpdates = [
            (["items", 3, "name"], "first"), (["items", 1], "second"),
            (["items", 3, "name"], nil), (["items", 1, "nested"], 7),
            (["profile"], ["name": "old"]), (["profile", "name"], "new"),
        ]
        var reference: Any? = nil
        for (route, value) in changes { reference[route] = value }
        let expected = try reference.json(.sortedKeys)
        let store = Any?.Store()
        await store.batch(changes)
        #expect(try await store.data.json(.sortedKeys) == expected)
        let sequential = Any?.Store()
        for (route, value) in changes { await sequential.set(route, to: value) }
        #expect(try await sequential.data.json(.sortedKeys) == expected)
    }

    @Test func concurrent_transactions_never_become_fake_nested_scopes_or_rollback_other_writes() async throws {
        let routed = Any?.Store()
        let typed = [String: Int].Store()
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<100 {
                group.addTask {
                    do {
                        try await routed.transaction { store in
                            #expect(store.transactionLevel == 1)
                            let previous = (try? store.get("counter", as: Int.self)) ?? 0
                            store.set("counter", to: previous + 1)
                            if index.isMultiple(of: 2) { throw TestFailure.rollback }
                        }
                    } catch {}
                    do {
                        try await typed.transaction { store in
                            #expect(store.transactionLevel == 1)
                            let previous = (try? store.get("counter")) ?? 0
                            store.set("counter", to: previous + 1)
                            if index.isMultiple(of: 2) { throw TestFailure.rollback }
                        }
                    } catch {}
                }
            }
        }
        #expect(try await routed.get("counter", as: Int.self) == 50)
        #expect(try await typed.get("counter") == 50)
    }

    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    @Test func cache_revisions_reject_late_writes_without_suppressing_other_mounts() async throws {
        let store = Any?.Store()
        let revisions = CacheRevisions()
        await store.cache([(["a"], "new")], revisions: revisions, revision: 2)
        await store.cache([(["a"], "old"), (["b"], "other")], revisions: revisions, revision: 1)
        #expect(try await store.get("a", as: String.self) == "new")
        #expect(try await store.get("b", as: String.self) == "other")
        revisions.close()
        await store.cache([(["a"], "closed")], revisions: revisions, revision: 3)
        #expect(try await store.get("a", as: String.self) == "new")
        var transient: CacheRevisions? = CacheRevisions()
        weak let released = transient
        await store.cache([(["c"], "temporary")], revisions: transient!, revision: 1)
        transient = nil
        #expect(released == nil)
    }
}

private enum TestFailure: Error { case rollback }
