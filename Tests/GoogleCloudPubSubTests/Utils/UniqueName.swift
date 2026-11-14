import Foundation

/// Returns a resource name unique to this call so suites and repeated runs against the same
/// emulator don't share topics or subscriptions.
func uniqueName(_ prefix: String) -> String {
  "\(prefix)-\(UUID().uuidString.lowercased())"
}
