# Challenge quality checklist

Every challenge needs three independent forms of evidence: a valid implementation
passes, a deliberately invalid implementation fails for the right reason, and
the same seed produces the same logical result. Unit coverage alone cannot prove
these properties. The public starter is a feasibility witness, not an optimum
or a substitute for adversarial testing.

## Shared acceptance checks

1. The statement, interface, starter, harness, and result metrics describe the
   same behavior. Every illustrated example must be hand-checkable.
2. Test empty inputs, one-item inputs, exact boundaries, maximum legal values,
   overflow paths, aliasing, repeated calls, and invalid calls where the
   contract defines their behavior.
3. Compare each small generated case with an independent, simple oracle. Vary
   seeds and sizes; avoid reusing the contestant implementation as the oracle.
4. Run a known-correct starter and at least one targeted broken implementation
   through the public harness. Assert the verdict and the relevant violation.
5. Repeat a seed and compare logical metrics and output. Measure runtime-derived
   allocation metrics separately; they are not deterministic logical counters.
6. Check that every advertised metric is affected by a contestant choice, or
   label it as workload context with zero scoring weight. Verify that a passing
   result has a meaningful score before enabling ranking.
7. Run the language entrypoint in the actual execution image. A local Go unit
   test does not establish that the browser's submitted source builds or runs.

## Per-challenge test design

| Challenge | Independent oracle and adversarial cases | Boundary and failure cases | What the learner should see |
| --- | --- | --- | --- |
| Cache Pressure, easy | Tiny map plus recency list; verify exact LRU order after hits and replacements. A never-admit cache and an eviction policy that removes the newest key must fail the contract probe. | Zero and exact capacity, oversized replacement, delete then read, repeated write, input/output aliasing, exact byte accounting. | A two-slot cache showing a hit moving a key to the newest position, followed by eviction of the other key. |
| Cache Pressure, medium | Independent byte-budget and expiry model. Compare pure LRU with a scan-resistant policy on the same hot-scan-hot trace; both must be correct while their miss costs differ. | Entry sizes at the budget, oversized replacement, TTL at and one tick before expiry, zero TTL, maximum-tick saturation, delete, aliasing. | Hot keys before and after a cold scan, with capacity and expiry shown separately. |
| Chunk Store | Byte-for-byte block/frame oracle; verify only touched blocks are read. Mutate a checksum, header length, and payload independently. | Empty and one-byte blobs, exact block boundaries, cross-block reads, duplicate key, delete, close, memory limit, truncated frame, decompression bomb. | A blob split into separately framed blocks and a short read touching only one block. |
| Deadline Scheduler | Enumerate short job sets and simulate a single non-preemptive worker. Compare FIFO with short-job-first where both are legal; check flow-time and starvation calculations. | Same-tick arrivals, idle gaps, zero/large work, completed/unknown job IDs, double placement, action while busy, clock overflow, no-progress policy. | A worker lane with arrival markers, waiting time, and a non-preemptive running interval. |
| Durable Work Queue | Sequential queue model for available and leased jobs, then concurrent producer/consumer traces checked for no loss or duplication. | Full/zero capacity, cancellation before admission, Nack, exact visibility deadline, stale tokens, expiry tie order, Close with blocked waiters, payload aliasing. | Available and leased lanes, with the visibility deadline returning a job to the queue. |
| Fair Gate | Rational arithmetic oracle for the token count and earliest retry tick. A limiter that over-admits or reports a late retry must fail. | Empty/full bucket, zero rate or burst, zero/oversized cost, same-tick requests, fractional refill, large tick jump, overflow, non-monotonic clock. | A token bucket at three ticks showing refill, an admitted cost, and a rejected retry. |
| Journal Recovery | Independent frame encoder/decoder and durable-prefix model. Crash after each write or sync position; recover exactly the valid prefix. | Empty log, every possible tail truncation, garbage tail, interior corruption, bad CRC, bad length, LSN gap/duplicate, callback error, caller mutation. | Physical frames, a sync frontier, and the difference between a torn tail and interior corruption. |
| Lossy Link | Compare the delivered byte stream with the original, independent of packet event order. Inject one fault at each send/ACK/timer point. | Empty and boundary payloads, lost/corrupt/duplicate/delayed data and ACK, stale timer, wraparound, packet limit, checksum, liveness timeout. | Sender and receiver lanes showing a lost packet, timer, retransmission, duplicate suppression, and ACK. |
| LSM Compactor | Small key-version oracle including tombstones; enumerate legal overlap-closed plans and compare read results before/after each merge. | Equal/max keys, empty and tombstone-only runs, overlapping L1 closure, stale/duplicate/distant IDs, oversized plan, no-op and always-compact policies. | Overlapping L0 runs, the exact L1 overlap set, and a merged run with one newest version per key. |
| Stream Windows | Simple per-key map grouped by `floor(tick/windowSize)`; compare canonical outputs after each watermark. | Tick zero and maximum legal tick, boundary events, repeated/regressing watermarks, empty windows, negative sums, interleaved keys, Close, duplicates, eviction. | Two adjacent event-time windows and a watermark that finalizes only the first. |

## Original SSD ladder

The three original challenges are defined in `web-platform/prisma/seed/challenges.ts`
and evaluated by `execution-runtime`:

| Challenge | Key proof and adversarial tests | Visual teaching goal |
| --- | --- | --- |
| Compaction | Prove the lower bound by counting live values outside the destination prefix. Verify legal moves preserve the multiset, reach the prefix, and score exactly at the bound; reject self moves, occupied destinations, missing moves, and duplicate data. Run the same solution through each language binding. | Show the prefix boundary, one legal move, and the resulting contiguous free suffix. |
| Victim Selection | Simulate seeded writes with an independent page map. Check valid block indices, migration accounting, no lost page, no stall, and correct write amplification. Compare at least greedy and a distinct legal policy; there is no unique best decision for unknown future traffic. | Show valid/dead/free pages and the migration cost of two possible victims. |
| Wear Levelling | Reuse victim-selection checks, then test hot-page thrash, capacity pressure, and interrupted migration with a fresh policy instance for each case. Measure both write amplification and worst-block wear. | Show a wear histogram beside the extra migration cost of avoiding the most-worn block. |

## Publication gate

Before a variant is advertised as ranked, require an independent reference or
calibration run, a versioned scoring rule, a nonzero score for a known-correct
solution, mutation tests for its correctness gates, and an end-to-end submitted
source run. Passing public smoke tests alone establish a working practice
exercise, not a competitive score.
