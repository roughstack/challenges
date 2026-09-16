// Package contract defines the narrow contestant-owned surface for the
// lsm-compactor-easy arena. The storage engine, merge implementation,
// correctness rules, accounting, and result construction are owned by the
// trusted harness.
package contract

// DefaultLevel0TriggerRuns is the published Level-0 run count at or above which
// a policy is expected to produce a compaction plan on the next flush.
const DefaultLevel0TriggerRuns uint64 = 4

// Config is the immutable per-run configuration handed to a policy.
type Config struct {
	// Level0TriggerRuns is the Level-0 run count threshold. A zero value falls
	// back to DefaultLevel0TriggerRuns.
	Level0TriggerRuns uint64
}

// Level0TriggerRunsEffective returns the configured threshold with the zero
// value resolved to the public default.
func (c Config) Level0TriggerRunsEffective() uint64 {
	if c.Level0TriggerRuns == 0 {
		return DefaultLevel0TriggerRuns
	}
	return c.Level0TriggerRuns
}

// RunMeta is the read-only metadata the harness publishes for one immutable
// sorted run. All fields are plain values; a policy must not assume it can
// mutate state through them.
type RunMeta struct {
	ID          uint64
	Level       uint64
	Bytes       uint64
	Entries     uint64
	Tombstones  uint64
	MinKey      uint64
	MaxKey      uint64
	CreatedTick uint64
}

// OverlapsKey reports whether key lies inside the run's key range. Empty runs
// never overlap a key.
func (r RunMeta) OverlapsKey(key uint64) bool {
	if r.Entries == 0 {
		return false
	}
	return key >= r.MinKey && key <= r.MaxKey
}

// OverlapsRange reports whether the run's key range intersects [minKey, maxKey].
// Empty runs never overlap a range. The caller must supply minKey <= maxKey.
func (r RunMeta) OverlapsRange(minKey, maxKey uint64) bool {
	if r.Entries == 0 {
		return false
	}
	return minKey <= r.MaxKey && r.MinKey <= maxKey
}

// OverlapsRun reports whether two runs have intersecting key ranges.
func (r RunMeta) OverlapsRun(other RunMeta) bool {
	if r.Entries == 0 || other.Entries == 0 {
		return false
	}
	return r.MinKey <= other.MaxKey && other.MinKey <= r.MaxKey
}

// State is a deep-copied, read-only snapshot of the LSM tree handed to Pick.
// Levels[0] is the unordered Level-0 run set and Levels[1] is the sorted
// Level-1 run set.
type State struct {
	Levels      [][]RunMeta
	DiskBytes   uint64
	BudgetBytes uint64
	NowTick     uint64
}

// Plan is a compaction request. InputRunIDs must reference existing runs and
// obey the public level rules; TargetLevel must be 1 for the easy variant.
type Plan struct {
	InputRunIDs []uint64
	TargetLevel uint32
}

// Policy is the complete contestant-owned surface for this arena.
//
// Pick receives a value snapshot of the current tree and returns a plan plus
// true when compaction is desired. A false return requests no work. Returning
// a stale, invalid, or oversized plan is a correctness failure; the harness
// rejects it instead of repairing it.
type Policy interface {
	Pick(state State) (Plan, bool)
}

// Factory constructs a fresh policy for one isolated workload run.
type Factory func(cfg Config) Policy
