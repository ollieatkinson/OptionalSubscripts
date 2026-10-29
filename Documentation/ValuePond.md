# ValuePond behaviour compatibility

Reference: [ValuePond implementation and tests](https://gist.github.com/ollieatkinson/9f2748231612483e0cfd5c2cf76d44e9),
revision `364b12981c80cda9008581cf2521de4ecdf8d7f1`.

The typed target and tests require Swift 6.4 / Xcode 27 and compile in Swift 6
language mode. Source and grace tasks explicitly enter with `@concurrent`;
asynchronous `defer` closes test ponds before each test returns. The original
untyped API is isolated in a Swift 5 compatibility target and re-exported.

The original tests define the timing, sharing and replay compatibility baseline.
This typed implementation and its expanded tests were developed in Namespace
before moving here. The suite uses snake_case names and Swift Testing, with
explicit source-start signals and TestClock rather than XCTest expectations or a
global serial-executor override.

| Original scenario | OptionalSubscripts coverage |
| --- | --- |
| Get a one-shot value and refresh after cache expiry | `one_shot_get_refreshes_only_after_the_grace_period` |
| Deliver every timer value, including many ticks in one advance | `a_timed_source_delivers_every_value_and_restarts_from_the_beginning` |
| Restart an expired timer from its initial state | Same timed-source test |
| Get the latest value while the source stays alive | `get_reads_the_latest_value_while_upstream_is_retained` |
| Stream an updating or mapped source to multiple consumers | `pending_subscribers_share_one_source_and_late_subscribers_get_replay`, `mapped_sequences_preserve_values_and_share_the_underlying_source` |
| Track independent subscribers as they leave | `multiple_cancellations_start_the_deadline_only_after_the_last_subscriber` |
| Cancel a pending stream or get with zero grace | Existing pending-subscription and pending-get cancellation tests |
| Do not cancel before a five-second grace period | `cancellation_waits_for_the_full_grace_period`, with get and stream cases |
| Keep upstream alive while one of several consumers remains | Multiple-cancellation test: ten subscribers reduced to one, then zero |

## Finite-source retention

The original ValuePond retains subscribers and the final value until they leave.
This implementation preserves that contract: an active subscriber prevents cache
expiry even after a finite source has produced its last value. A test advances
four grace periods while that subscriber remains and verifies the value is still
shared.

## Explicit extensions

- An empty source finishes rather than leaving a get suspended indefinitely.
- A get already cancelled before acquisition starts no upstream work. Tests that
  expect an upstream termination first establish that the upstream actually began.
- Owner teardown closes subscribers and cancels upstream work.
- Source generations reject late results from a cancelled factory.
- Buffering applies directly to each consumer, avoiding an unbounded forwarding
  stream around a bounded buffer.

These extensions have separate tests. They do not replace the original timing,
sharing and replay scenarios.
