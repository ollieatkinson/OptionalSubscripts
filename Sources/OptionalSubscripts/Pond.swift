import Foundation

public extension Optional where Wrapped == Any {
    /// Projects live coarse Geyser documents into fine routed observations.
    /// Different routes sharing a GushID share one upstream subscription. Mapping
    /// changes release only that observer's old lease; other observers stay live.
    @available(macOS 15, iOS 18, tvOS 18, watchOS 11, *)
    actor Pond<Source: Geyser> {
        public typealias Route = Any?.Route
        public typealias Location = Any?.Location
        public typealias Stream = AsyncThrowingStream<SendableValue, Error>
        public typealias BufferingPolicy = Stream.Continuation.BufferingPolicy

        public let source: Source
        /// Inspectable source cache. Edits here do not feed observations back
        /// into the Geyser; edit its backing source for live document changes.
        public let store: Any?.Store
        private var gracePeriod: Duration
        private let sleep: @Sendable (Duration) async throws -> Void
        private var generation: UInt64 = 0
        private var revision: UInt64 = 0
        private var cacheRevisions = CacheRevisions()
        private var cacheMounts: [Route: Source.GushID] = [:]
        private var subscriptions: [UUID: Subscription] = [:]
        private var cacheWriteBarrier: (@Sendable () async -> Void)?
        private var entries: [Source.GushID: Entry] = [:]

        private struct Subscription {
            let route: Route
            let output: Stream.Continuation
            var mapping: (id: Source.GushID, route: Route)?
            var mappingOrder: UInt64 = 0
        }
        private enum Replay { case pending, value(SendableValue) }
        private struct Entry {
            let generation: UInt64
            var observers: Set<UUID> = []
            var replay = Replay.pending
            var task: Task<Void, Never>?
            var grace: Task<Void, Never>?
            var lease: UInt64 = 0
        }
        public struct GushSource: Sendable {
            public let referenceCount: UInt
        }
        public var gushSources: [Source.GushID: GushSource] {
            entries.mapValues { GushSource(referenceCount: UInt($0.observers.count)) }
        }
        public var sourceIDs: [Route: Source.GushID] { cacheMounts }
        public enum RoutingError: Error, Equatable {
            case unmounted(Route)
            case invalidMapping(Route)
        }

        public init(
            source: Source,
            store: Any?.Store = .init(),
            cancellingGracePeriod: Duration = .zero,
            clock: some Clock<Duration> = ContinuousClock()
        ) {
            self.source = source
            self.store = store
            self.gracePeriod = max(.zero, cancellingGracePeriod)
            self.sleep = { try await clock.sleep(for: $0) }
        }
        deinit {
            cacheRevisions.close()
            for entry in entries.values { entry.task?.cancel(); entry.grace?.cancel() }
            for subscription in subscriptions.values { subscription.output.finish() }
        }

        public func setCancellingGracePeriod(to seconds: Double) {
            gracePeriod = .seconds(max(0, seconds))
        }

        public nonisolated func stream(_ route: Location..., bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> Stream {
            stream(route, bufferingPolicy: bufferingPolicy)
        }

        public nonisolated func stream<Route: Collection>(_ route: Route, bufferingPolicy: BufferingPolicy = .bufferingNewest(1)) -> Stream where Route.Element == Location {
            let route = Array(route)
            let source = source
            return Stream(bufferingPolicy: bufferingPolicy) { output in
                let id = UUID()
                let task = Task { @concurrent [weak self] in
                    guard !Task.isCancelled else { output.finish(); return }
                    await self?.register(id, route: route, output: output)
                    guard self != nil, !Task.isCancelled else { output.finish(); return }
                    let mappings = await source.source(of: route)
                    var iterator = mappings.makeAsyncIterator()
                    var completed = false
                    defer {
                        if !completed, Task.isCancelled {
                            _ = try? await iterator.next(isolation: nil)
                        }
                    }
                    do {
                        while let mapping = try await iterator.next(isolation: nil) {
                            guard !Task.isCancelled else { return }
                            guard let self else { output.finish(); return }
                            do { try await self.remap(id, to: mapping) }
                            catch {
                                await self.finish(id, throwing: error)
                                return
                            }
                        }
                        completed = true
                        await self?.mappingCompleted(id)
                    } catch {
                        completed = true
                        await self?.finish(id, throwing: error)
                    }
                }
                output.onTermination = { @Sendable [weak self] _ in
                    task.cancel()
                    Task { @concurrent in await self?.remove(id) }
                }
            }
        }

        public func cancel() {
            cacheRevisions.close()
            cacheRevisions = CacheRevisions()
            let outputs = subscriptions.values.map(\.output)
            subscriptions.removeAll()
            for entry in entries.values { entry.task?.cancel(); entry.grace?.cancel() }
            entries.removeAll()
            cacheMounts.removeAll()
            for output in outputs { output.finish() }
        }

        private func mappingCompleted(_ id: UUID) {
            if subscriptions[id]?.mapping == nil { finish(id) }
        }

        private func register(_ id: UUID, route: Route, output: Stream.Continuation) {
            guard !Task.isCancelled else { output.finish(); return }
            subscriptions[id] = Subscription(route: route, output: output)
        }

        private func remap(_ id: UUID, to mapping: (id: Source.GushID, route: Route)?) async throws {
            guard var subscription = subscriptions[id] else { return }
            guard let mapping else { throw RoutingError.unmounted(subscription.route) }
            guard subscription.route.starts(with: mapping.route) else { throw RoutingError.invalidMapping(mapping.route) }
            let previous = subscription.mapping
            if let previous, previous.id == mapping.id, previous.route == mapping.route { return }
            revision &+= 1
            let mappingRevision = revision
            let affectedRoutes = [mapping.route] + (previous.map { [$0.route] } ?? [])
            subscription.mapping = mapping
            subscription.mappingOrder = revision
            subscriptions[id] = subscription
            if let previous, previous.route != mapping.route { refreshCacheMount(previous.route) }
            cacheMounts[mapping.route] = mapping.id
            // Changing a projection on the same document must not restart its
            // upstream, even when this is its only observer and grace is zero.
            if let previous, previous.id == mapping.id {
                cacheRevisions.invalidate(affectedRoutes, revision: mappingRevision)
                if case .value(let value) = entries[mapping.id]?.replay {
                    subscription.output.yield(value[subscription.route.dropFirst(mapping.route.count)])
                    await store.cache([(mapping.route, value)], revisions: cacheRevisions, revision: mappingRevision)
                }
                return
            }
            if let previous { release(id, from: previous.id) }
            if entries[mapping.id] == nil {
                generation &+= 1
                entries[mapping.id] = Entry(generation: generation)
                start(mapping.id, generation: generation)
            }
            entries[mapping.id]?.grace?.cancel()
            entries[mapping.id]?.grace = nil
            entries[mapping.id]?.lease &+= 1
            entries[mapping.id]?.observers.insert(id)
            cacheRevisions.invalidate(affectedRoutes, revision: mappingRevision)
            if case .value(let value) = entries[mapping.id]?.replay {
                subscription.output.yield(value[subscription.route.dropFirst(mapping.route.count)])
                await store.cache([(mapping.route, value)], revisions: cacheRevisions, revision: mappingRevision)
            }
        }

        // A gate allows lifecycle tests to hold a pending cache hop while a
        // source remaps. The production path has no additional suspension.
        func setCacheWriteBarrier(_ barrier: (@Sendable () async -> Void)?) { cacheWriteBarrier = barrier }

        private func start(_ id: Source.GushID, generation: UInt64) {
            let source = source
            entries[id]?.task = Task { @concurrent [weak self] in
                let stream = await source.stream(id)
                var iterator = stream.makeAsyncIterator()
                var completed = false
                defer {
                    if !completed, Task.isCancelled { _ = try? await iterator.next(isolation: nil) }
                }
                guard !Task.isCancelled else { return }
                do {
                    while let value = try await iterator.next(isolation: nil) {
                        guard !Task.isCancelled else { return }
                        await self?.publish(value, from: id, generation: generation)
                    }
                    completed = true
                    await self?.completed(id, generation: generation)
                } catch {
                    completed = true
                    guard !Task.isCancelled else { return }
                    await self?.failed(id, generation: generation, error: error)
                }
            }
        }

        private func publish(_ value: SendableValue, from id: Source.GushID, generation: UInt64) async {
            guard entries[id]?.generation == generation else { return }
            entries[id]?.replay = .value(value)
            var mounts: Set<Route> = []
            for observer in entries[id]?.observers ?? [] {
                guard let subscription = subscriptions[observer], let mapping = subscription.mapping else { continue }
                subscription.output.yield(value[subscription.route.dropFirst(mapping.route.count)])
                if cacheMounts[mapping.route] == id { mounts.insert(mapping.route) }
            }
            // Keep the inspectable document cache, without using a second
            // forwarding stream or buffer for each fine-route observer.
            revision &+= 1
            let publication = revision
            let revisions = cacheRevisions
            if let cacheWriteBarrier { await cacheWriteBarrier() }
            guard !Task.isCancelled, entries[id]?.generation == generation else { return }
            let currentMounts = mounts.filter { cacheMounts[$0] == id }
            await store.cache(currentMounts.map { ($0, value) }, revisions: revisions, revision: publication)
        }

        private func completed(_ id: Source.GushID, generation: UInt64) {
            guard let entry = entries[id], entry.generation == generation else { return }
            entries[id]?.task = nil
            if case .pending = entry.replay {
                for observer in entry.observers { finish(observer) }
                reset(id)
            }
        }
        private func failed(_ id: Source.GushID, generation: UInt64, error: Error) {
            guard let entry = entries[id], entry.generation == generation else { return }
            for observer in entry.observers { finish(observer, throwing: error) }
            reset(id)
        }
        private func finish(_ id: UUID, throwing error: Error? = nil) {
            let output = subscriptions[id]?.output
            remove(id)
            output?.finish(throwing: error)
        }
        private func remove(_ id: UUID) {
            guard let subscription = subscriptions.removeValue(forKey: id), let mapping = subscription.mapping else { return }
            release(id, from: mapping.id)
            refreshCacheMount(mapping.route)
        }
        private func refreshCacheMount(_ route: Route) {
            cacheMounts[route] = subscriptions.values.filter { $0.mapping?.route == route }
                .max(by: { $0.mappingOrder < $1.mappingOrder })?.mapping?.id
        }
        private func release(_ observer: UUID, from id: Source.GushID) {
            guard entries[id]?.observers.remove(observer) != nil, let entry = entries[id], entry.observers.isEmpty else { return }
            guard gracePeriod > .zero else { reset(id); return }
            entries[id]?.lease &+= 1
            let lease = entries[id]?.lease
            let grace = gracePeriod
            entries[id]?.grace = Task { @concurrent [weak self, sleep] in
                do { try await sleep(grace) } catch { return }
                guard !Task.isCancelled else { return }
                await self?.expire(id, generation: entry.generation, lease: lease)
            }
        }
        private func expire(_ id: Source.GushID, generation: UInt64, lease: UInt64?) {
            guard let entry = entries[id], entry.generation == generation,
                  entry.lease == lease, entry.observers.isEmpty else { return }
            reset(id)
        }
        private func reset(_ id: Source.GushID) {
            let entry = entries.removeValue(forKey: id)
            entry?.task?.cancel()
            entry?.grace?.cancel()
        }
    }
}
