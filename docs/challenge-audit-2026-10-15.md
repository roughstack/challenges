# Challenge audit, 2026-10-15

This is the current evidence, not a claim that finite tests prove every possible
implementation correct. The companion [quality matrix](challenge-quality.md)
lists the adversarial and boundary tests each challenge should ultimately own.

## Evidence checked

- All ten public Go baselines passed their smoke workloads at seeds `12345` and
  `987654321`. The full Go suite passed with the race detector.
- All ten baselines were submitted through the web platform's production build
  and completed with passing verdicts in an isolated local database. This caught
  a platform result-schema mismatch: Journal Recovery and Lossy Link include
  the optional `run.fault_profile` field, which the platform now accepts.
- The original SSD execution runtime's Go and conformance suites passed.
- All thirteen challenge pages rendered an accessible educational diagram in
  the browser. The web unit suite, a phone-sized problem/editor interaction
  check, and the existing 40-viewport layout audit passed.
- The easy cache now has a standalone contract probe that rejects an incorrect
  LRU, accounting, aliasing, deletion, or oversized-replacement implementation.
  The medium cache now enforces its advertised 40% request-hit floor on the
  default public workload, so a never-admit cache fails.
- A production node needs both the published arena registry and access to the
  matching challenge packages. Registry-only mode can display previously
  materialized database content, but cannot refresh statements or execute a
  package that is unavailable locally.
- Follow-up visual review found that the browser was prefilled with the passing
  baseline. The manifests now declare a separate `submission.starter`, and
  all ten packages provide incomplete learner scaffolds while retaining the
  public baseline at `submission.entrypoint`. The scaffold path is validated
  separately from the file overwritten during a run.
- Version `1.0.1` of every harness now matches its manifest after the scaffold
  release. A real submission of the easy-cache public baseline at this version
  passed through the platform execution API; submitting the untouched learner
  scaffold failed its public tests as expected. The platform's arena validator
  checks Go harness and manifest versions for future drift. Browser checks
  confirmed that all ten practice editors display unfinished code, and the
  expanded visual and responsive suites passed.

## Challenge-by-challenge conclusion

| Challenge | Does the problem have a coherent solution? | Public evidence and next gap |
| --- | --- | --- |
| Cache Pressure, easy | Yes. Exact LRU behavior is prescribed; implementations need not share a data structure. | Baseline and contract probe pass. Add a small independent model across randomized operation sequences and a candidate that lies about `UsedBytes` while retaining data. |
| Cache Pressure, medium | Yes. Many admission/eviction policies are legal; the public workload now requires at least 40% request hits. | Baseline passes the floor across the checked seeds; never-admit fails. Add a model-based TTL/accounting probe independent of the generated workload, especially replacement at an exact expiry tick. |
| Chunk Store | Yes. Bytes returned and frame-local integrity are definite even though internal indexing can differ. | Corrupt length, checksum, over-allocation, unrelated reads, and reclaim tests exist. Expand every-tail-truncation and cross-block read cases under a tiny independent frame oracle. |
| Deadline Scheduler | Yes. Many fair, non-preemptive schedules are valid; no single order is mandated. | Arrival, ties, aging, idle gaps, failure gating, and starter tests pass. Add exhaustive short-job schedule comparison to validate flow-time accounting and starvation bounds. |
| Durable Work Queue | Yes. The visible lease/ack/nack behavior is definite; lock and storage choices are not. | Correctness, panic, nil, and watchdog tests pass. Add a concurrent history checker for no loss or duplication and more exact visibility-deadline ties. |
| Fair Gate | Yes. Given a token bucket config and request trace, allow/deny and retry tick are mathematically determined. | Tests reject over-admission, over-charging, and wrong retry ticks. Add an independent rational-arithmetic oracle for randomly generated short traces and overflow boundaries. |
| Journal Recovery | Yes. Recovery must return the valid durable prefix, regardless of internal data structure. | Every byte-tail truncation of a short encoded log, plus LSN gaps/duplicates, callback errors, corruption, panic, and nil cases pass. Add more interior-versus-tail fault pairs and an independent prefix oracle. |
| Lossy Link | Yes. Delivered bytes are fixed; retransmission schedule and packet count can vary. | Loss, corruption, duplicate, stale ACK, timer, wrap, and boundary tests pass. Add a bounded fault-sequence enumerator to check safety and liveness against many combined faults. |
| LSM Compactor | Yes. Several legal plans exist; reads after merging must preserve newest-version and tombstone semantics. | Invalid overlap closure, stale/duplicate plans, tombstones, empty runs, and tie-breaking pass. Add an independent key-version map oracle over enumerated small run graphs. |
| Stream Windows | Yes. Half-open window assignment and finalized sums are unique for a legal event trace. | Boundary, regression, duplicate/premature output, negative sum, close, and max-tick tests pass. Add randomized per-key map oracle and repeated watermark/event interleavings. |
| Compaction, original SSD | Yes. A minimum move count exists, though more than one move sequence can attain it. | Runtime tests demonstrate a legal optimal solution and reject illegal moves. Retain cross-language submitted-source conformance as a release gate. |
| Victim Selection, original SSD | Yes. Any legal policy preserves data; the best online choice depends on future writes. | Runtime simulation and conformance pass. Add property tests for page-map conservation and migration accounting under varied seeds. |
| Wear Levelling, original SSD | Yes. Legal policies trade migration cost against erase-count spread; there is no single mandated policy. | Runtime simulation and conformance pass. Add adversarial hot/cold workloads and explicit Pareto trade-off checks. |

## Publication limits

All ten generated public smoke results currently report `score: 0`, including
correct baselines. Their weighted scoring files are design intent, not an active
ranking system. Some displayed measurements, notably fair-gate throughput and
arithmetic work and stream-window event throughput/output delay, are fixed by
the public trace once the answer is correct; they should be labeled as trace
context until a contestant-sensitive measurement replaces them. Generated
challenges currently ship Go only, whereas the original SSD ladder has the
multi-language runtime. Do not advertise the generated set as ranked or
multi-language until those separate gates are implemented and tested.
