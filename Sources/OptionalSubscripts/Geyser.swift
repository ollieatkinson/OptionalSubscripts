/// A coarse document source plus a live mapping from fine routes to documents.
public protocol Geyser: Sendable {
    associatedtype GushID: Hashable & Sendable
    associatedtype Gushes: AsyncSequence & Sendable where Gushes.Element == SendableValue
    associatedtype GushToRouteMapping: AsyncSequence & Sendable
        where GushToRouteMapping.Element == (id: GushID, route: Any?.Route)?

    func stream(_ gush: GushID) async -> Gushes
    func source(of route: Any?.Route) async -> GushToRouteMapping
}
