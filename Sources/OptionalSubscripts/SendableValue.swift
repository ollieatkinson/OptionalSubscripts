import Foundation

/// The routed store's payload. `nil` means absence; `NSNull()` is present null.
/// Scalars and containers must be statically Sendable before entering an actor.
public typealias SendableValue = (any Sendable)?

public extension Optional where Wrapped == any Sendable {
    subscript(_ route: Any?.Location...) -> SendableValue {
        get { self[route] }
        set { self[route] = newValue }
    }

    subscript<Route: Collection>(_ route: Route) -> SendableValue where Route.Element == Any?.Location {
        get {
            var value = self
            for location in route { value = value[location] }
            return value
        }
        set {
            guard let location = route.first else { self = newValue; return }
            var child = self[location]
            child[route.dropFirst()] = newValue
            self[location] = child
        }
    }

    subscript(_ location: Any?.Location) -> SendableValue {
        get {
            switch location {
            case .key(let key): return (self as? [String: any Sendable])?[key]
            case .index(let index):
                guard let array = self as? [SendableValue], array.indices.contains(index) else { return nil }
                return array[index]
            }
        }
        set {
            switch location {
            case .key(let key):
                var object = self as? [String: any Sendable] ?? [:]
                object[key] = newValue
                self = object.isEmpty ? nil : object
            case .index(let index):
                guard index >= 0 else { return }
                var array = self as? [SendableValue] ?? []
                array.append(contentsOf: repeatElement(nil, count: max(0, index - array.count + 1)))
                array[index] = newValue
                while let last = array.last, last == nil { array.removeLast() }
                self = array.isEmpty ? nil : array
            }
        }
    }

    func cast<Value>(to: Value.Type = Value.self) throws -> Value {
        guard let value = self as? Value else { throw CastingError(value: self, to: Value.self) }
        return value
    }

    func json(_ options: JSONSerialization.WritingOptions = [.fragmentsAllowed, .sortedKeys, .prettyPrinted]) throws -> Data {
        let value: Any? = self.map { $0 as Any }
        return try value.json(options)
    }

    /// Converts a JSONSerialization-style tree synchronously, before an actor hop.
    /// Arbitrary custom values must enter through a statically Sendable type.
    init(jsonObject: Any) throws {
        switch jsonObject {
        case let value as String: self = value
        case let value as NSNumber: self = value
        case is NSNull: self = NSNull()
        case let values as [Any]: self = try values.map { try SendableValue(jsonObject: $0) }
        case let values as [String: Any]:
            self = try values.mapValues { try SendableValue(jsonObject: $0) }
        default: throw JSONConversionError.unsupportedType(String(reflecting: type(of: jsonObject)))
        }
    }
}

public enum JSONConversionError: Error, Equatable {
    case unsupportedType(String)
}
