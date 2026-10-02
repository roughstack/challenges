# Lossy Link

Transfer a byte stream across a deterministic simulated network that may lose,
duplicate, delay, and corrupt packets. The contestant owns only the sender and
receiver protocol state; the harness owns the event loop, virtual clock,
application stream, fault injection, accounting, and result.

## Scenario

The application hands the sender a fixed-length byte stream. The sender must
deliver every byte to the receiver exactly once and in order, using data
packets that carry a bounded payload, a sequence number, and a checksum. The
receiver acknowledges the last valid sequence and discards duplicates. A packet
that is lost, delayed, or corrupted must be recovered by retransmission rather
than by delivering damaged or duplicate data.

This easy variant allows at most one unacknowledged data packet at a time, so
a stop-and-wait protocol with a small wrapping sequence number is sufficient
and correct.

## Non-goals

- No sliding windows, cumulative/selective acknowledgements, flow control, or
  congestion control. Those belong to the medium and hard variants.
- No real networking, goroutines, wall-clock timers, or environment access.
  Time is a virtual tick owned by the harness.
- No adaptive timeout estimation. A fixed retransmission timeout is expected
  and sufficient here.
- No hidden workloads, ranked seeds, or reference implementations in this
  repository.

## Endpoint contract

Implement the `contract.Endpoint` interface and expose these constructors:

```go
func NewSender(cfg contract.Config) contract.Endpoint
func NewReceiver(cfg contract.Config) contract.Endpoint
```

The full surface lives in `contract/contract.go`. The relevant roles:

- `OnAppData(data []byte, eof bool) []Packet` — the sender receives another
  chunk of the application stream; `eof` is true on the final chunk.
- `OnPacket(pkt Packet, nowTick uint64) []Packet` — deliver a packet arriving
  from the peer and return any response packets.
- `OnTimer(timerID uint64, nowTick uint64) []Packet` — fire a timer previously
  reported by `NextTimers`.
- `ReadDelivered(max int) []byte` — the receiver returns and consumes committed,
  in-order bytes.
- `NextTimers() []Timer` — report currently pending timers.
- `StateBytes() uint64` — report current protocol-state size.
- `Done() bool` — report that this endpoint has finished its side.

`Packet` carries a kind (data or ack), a small wrapping sequence number, a
small wrapping acknowledgement, a bounded payload, and a checksum. The harness
rejects any packet whose framing plus payload exceeds
`contract.MaxPacketSize(cfg.MaxPayload())` and rejects payloads longer than the
configured maximum. Returned payloads and accepted app data must be copies,
never aliases of caller buffers.

## Metrics

- `completion_ticks` — virtual ticks until the transfer completes (minimize).
- `transmitted_bytes` — application bytes placed on the wire, including
  retransmissions (minimize).
- `packet_count` — packets emitted by the endpoints (minimize).
- `retransmissions` — data packets the sender re-emitted after an earlier
  transmission (minimize).
- `state_bytes` — peak combined protocol-state bytes reported by both endpoints
  (minimize).

Correctness is a gate: a panic, timeout, malformed result, liveness failure, or
delivered stream that does not match the source produces a failed verdict and a
score of zero. Public smoke output is reproducible but unofficial; normalization
and calibration belong to the platform and private judge.

## Verify

From the repository root:

```sh
go fmt ./arenas/lossy-link-easy/...
go test ./arenas/lossy-link-easy/...
go vet ./arenas/lossy-link-easy/...
go test ./...
go run ./arenas/lossy-link-easy/cmd/smoke --seed 1844674407370955161
```

Run the smoke command twice with the same seed; the logical counters must be
identical. The command writes exactly one `bytearena.result/v1` JSON object to
stdout and uses no wall time, network access, environment secrets, or random
seeds.
