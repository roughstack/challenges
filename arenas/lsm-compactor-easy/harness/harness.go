// Package harness owns public execution, the deterministic storage engine,
// logical accounting, plan validation, and result construction for the
// lsm-compactor-easy arena. The contestant surface is the contract.Policy
// only.
package harness

import (
	"sort"
	"strconv"

	"github.com/bytearena/arenas/arenas/lsm-compactor-easy/contract"
	"github.com/bytearena/arenas/arenas/lsm-compactor-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "lsm-compactor-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"

	entryOverheadBytes uint64 = 16
	maxPlanInputs             = 128
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	ReadAmplificationBPS uint64 `json:"read_amplification_bps"`
	BytesRewritten       uint64 `json:"bytes_rewritten"`
	CompactionStallTicks uint64 `json:"compaction_stall_ticks"`
	PeakDiskBytes        uint64 `json:"peak_disk_bytes"`
}

// RunInfo records protocol-required run metadata without wall-clock noise.
type RunInfo struct {
	WorkloadID      string `json:"workload_id"`
	DurationNS      uint64 `json:"duration_ns"`
	PeakMemoryBytes uint64 `json:"peak_memory_bytes"`
}

// Result is the versioned one-object stdout protocol.
type Result struct {
	ProtocolVersion int      `json:"protocol_version"`
	ArenaID         string   `json:"arena_id"`
	ArenaVersion    string   `json:"arena_version"`
	Seed            string   `json:"seed"`
	Verdict         string   `json:"verdict"`
	Score           int      `json:"score"`
	Metrics         Metrics  `json:"metrics"`
	Violations      []string `json:"violations"`
	Run             RunInfo  `json:"run"`
}

// PublicConfig is intentionally small and distinct from private ranked inputs.
type PublicConfig struct {
	Operations         int
	KeySpace           uint64
	ValueBytes         uint64
	ScanLength         int
	MemtableFlushBytes uint64
	Level0TriggerRuns  uint64
	MaxLevel0Runs      uint64
	BudgetBytes        uint64
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	const valueBytes uint64 = 64
	return PublicConfig{
		Operations:         2000,
		KeySpace:           256,
		ValueBytes:         valueBytes,
		ScanLength:         16,
		MemtableFlushBytes: (valueBytes + entryOverheadBytes) * 8,
		Level0TriggerRuns:  contract.DefaultLevel0TriggerRuns,
		MaxLevel0Runs:      8,
		BudgetBytes:        1 << 20,
	}
}

// Evaluate runs the deterministic public workload against a contestant policy
// and returns the versioned result envelope.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	config, policyCfg := normalizeConfig(config)
	sim := newSimulator(seed, config, factory, policyCfg)
	sim.run()
	return sim.result()
}

func normalizeConfig(config PublicConfig) (PublicConfig, contract.Config) {
	if config.Operations <= 0 {
		config.Operations = 2000
	}
	if config.KeySpace == 0 {
		config.KeySpace = 256
	}
	if config.ValueBytes == 0 {
		config.ValueBytes = 64
	}
	if config.ScanLength <= 0 {
		config.ScanLength = 16
	}
	if config.Level0TriggerRuns == 0 {
		config.Level0TriggerRuns = contract.DefaultLevel0TriggerRuns
	}
	if config.MaxLevel0Runs == 0 {
		if config.Level0TriggerRuns > ^uint64(0)/2 {
			config.MaxLevel0Runs = ^uint64(0)
		} else {
			config.MaxLevel0Runs = config.Level0TriggerRuns * 2
		}
	}
	if config.BudgetBytes == 0 {
		config.BudgetBytes = 1 << 20
	}
	if config.MemtableFlushBytes == 0 {
		config.MemtableFlushBytes = satMul(satAdd(config.ValueBytes, entryOverheadBytes), 8)
	}

	policyCfg := contract.Config{Level0TriggerRuns: config.Level0TriggerRuns}
	return config, policyCfg
}

type entry struct {
	key       uint64
	version   uint64
	tombstone bool
	bytes     uint64
}

type run struct {
	id         uint64
	level      uint32
	created    uint64
	entries    []entry
	bytes      uint64
	tombstones uint64
}

func newRun(id uint64, level uint32, created uint64, entries []entry) *run {
	r := &run{id: id, level: level, created: created, entries: entries}
	for _, e := range entries {
		r.bytes = satAdd(r.bytes, e.bytes)
		if e.tombstone {
			r.tombstones++
		}
	}
	return r
}

func (r *run) hasKeys() bool { return len(r.entries) > 0 }

func (r *run) minKey() uint64 {
	if !r.hasKeys() {
		return 0
	}
	return r.entries[0].key
}

func (r *run) maxKey() uint64 {
	if !r.hasKeys() {
		return 0
	}
	return r.entries[len(r.entries)-1].key
}

func (r *run) overlapsKey(key uint64) bool { return r.overlapsRange(key, key) }

func (r *run) overlapsRange(minKey, maxKey uint64) bool {
	if !r.hasKeys() {
		return false
	}
	return minKey <= r.maxKey() && r.minKey() <= maxKey
}

func (r *run) findEntry(key uint64) (entry, bool) {
	index := sort.Search(len(r.entries), func(i int) bool { return r.entries[i].key >= key })
	if index < len(r.entries) && r.entries[index].key == key {
		return r.entries[index], true
	}
	return entry{}, false
}

func (r *run) meta() contract.RunMeta {
	return contract.RunMeta{
		ID:          r.id,
		Level:       uint64(r.level),
		Bytes:       r.bytes,
		Entries:     uint64(len(r.entries)),
		Tombstones:  r.tombstones,
		MinKey:      r.minKey(),
		MaxKey:      r.maxKey(),
		CreatedTick: r.created,
	}
}

type simulator struct {
	seed      uint64
	config    PublicConfig
	factory   contract.Factory
	policyCfg contract.Config
	policy    contract.Policy

	nowTick       uint64
	nextRunID     uint64
	levels        [2][]*run
	memtable      []entry
	memtableBytes uint64
	latest        map[uint64]entry

	logicalReadBytes uint64
	requestedBytes   uint64
	bytesRewritten   uint64
	compactionStalls uint64
	peakDiskBytes    uint64
	flushCount       uint64
	violations       []string
}

func newSimulator(seed uint64, config PublicConfig, factory contract.Factory, policyCfg contract.Config) *simulator {
	return &simulator{
		seed:      seed,
		config:    config,
		factory:   factory,
		policyCfg: policyCfg,
		latest:    make(map[uint64]entry),
	}
}

func (s *simulator) run() {
	if s.factory == nil {
		s.addViolation("policy factory is nil")
		return
	}
	if !s.call(func() { s.policy = s.factory(s.policyCfg) }) {
		return
	}
	if s.policy == nil {
		s.addViolation("policy factory returned a nil policy")
		return
	}

	operations := workload.Generate(workload.Config{
		Seed:       s.seed,
		Operations: s.config.Operations,
		KeySpace:   s.config.KeySpace,
		ScanLength: s.config.ScanLength,
	})

	for _, operation := range operations {
		s.applyOperation(operation)
		if len(s.violations) >= 8 {
			break
		}
	}

	if len(s.violations) < 8 && s.memtableBytes > 0 {
		s.flush()
	}
	s.sampleDisk()
}

func (s *simulator) applyOperation(operation workload.Operation) {
	switch operation.Kind {
	case workload.Write:
		s.write(operation.Key)
	case workload.Read:
		s.read(operation.Key)
	case workload.Delete:
		s.delete(operation.Key)
	case workload.Scan:
		s.scan(operation.Key, operation.Length)
	case workload.Snapshot:
		s.snapshot()
	default:
		s.addViolation("unknown workload operation")
	}
}

func (s *simulator) tick() {
	if s.nowTick < ^uint64(0) {
		s.nowTick++
	}
}

func (s *simulator) write(key uint64) {
	s.tick()
	value := entry{
		key:     key,
		version: s.nextVersion(key),
		bytes:   s.putEntryBytes(),
	}
	s.latest[key] = value
	s.memtable = append(s.memtable, value)
	s.memtableBytes = satAdd(s.memtableBytes, value.bytes)
	s.maybeFlush()
}

func (s *simulator) delete(key uint64) {
	s.tick()
	tombstone := entry{
		key:       key,
		version:   s.nextVersion(key),
		tombstone: true,
		bytes:     s.tombstoneEntryBytes(),
	}
	s.latest[key] = tombstone
	s.memtable = append(s.memtable, tombstone)
	s.memtableBytes = satAdd(s.memtableBytes, tombstone.bytes)
	s.maybeFlush()
}

func (s *simulator) read(key uint64) {
	s.tick()
	s.requestedBytes = satAdd(s.requestedBytes, s.config.ValueBytes)
	actual, actualOK := s.lsmRead(key)
	expected, expectedOK := s.latest[key]
	if actualOK != expectedOK || (actualOK && !sameEntry(actual, expected)) {
		s.addViolation("storage engine read did not match reference state")
		return
	}
	s.sampleDisk()
}

func (s *simulator) scan(start uint64, length int) {
	s.tick()
	if length <= 0 {
		return
	}
	if start > ^uint64(0)-uint64(length-1) {
		s.addViolation("scan range overflow")
		return
	}
	s.requestedBytes = satAdd(s.requestedBytes, satMul(uint64(length), s.config.ValueBytes))
	for offset := 0; offset < length; offset++ {
		key := start + uint64(offset)
		actual, actualOK := s.lsmRead(key)
		expected, expectedOK := s.latest[key]
		if actualOK != expectedOK || (actualOK && !sameEntry(actual, expected)) {
			s.addViolation("storage engine scan did not match reference state")
			return
		}
	}
	s.sampleDisk()
}

func (s *simulator) snapshot() {
	s.tick()
	keys := make([]uint64, 0, len(s.latest))
	for key := range s.latest {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	s.requestedBytes = satAdd(s.requestedBytes, satMul(uint64(len(keys)), s.config.ValueBytes))
	for _, key := range keys {
		actual, actualOK := s.lsmRead(key)
		expected, expectedOK := s.latest[key]
		if actualOK != expectedOK || (actualOK && !sameEntry(actual, expected)) {
			s.addViolation("storage engine snapshot did not match reference state")
			return
		}
	}
	s.sampleDisk()
}

func (s *simulator) nextVersion(key uint64) uint64 {
	if existing, ok := s.latest[key]; ok {
		return existing.version + 1
	}
	return 1
}

func (s *simulator) putEntryBytes() uint64 {
	return satAdd(s.config.ValueBytes, entryOverheadBytes)
}

func (s *simulator) tombstoneEntryBytes() uint64 {
	return entryOverheadBytes
}

func (s *simulator) maybeFlush() {
	if s.memtableBytes >= s.config.MemtableFlushBytes {
		s.flush()
	}
}

func (s *simulator) flush() {
	if len(s.memtable) == 0 {
		return
	}
	s.flushCount++

	byKey := make(map[uint64]entry, len(s.memtable))
	for _, e := range s.memtable {
		byKey[e.key] = e
	}
	entries := make([]entry, 0, len(byKey))
	for _, e := range byKey {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	run := newRun(s.nextRunID, 0, s.nowTick, entries)
	s.nextRunID++
	s.levels[0] = append(s.levels[0], run)
	s.memtable = s.memtable[:0]
	s.memtableBytes = 0

	s.sampleDisk()
	s.considerCompaction()
}

func (s *simulator) considerCompaction() {
	if s.policy == nil {
		return
	}

	state := s.state()
	var plan contract.Plan
	var ok bool
	if !s.call(func() { plan, ok = s.policy.Pick(state) }) {
		return
	}
	if !ok {
		s.checkInvariants()
		return
	}

	if violation := s.validatePlan(plan); violation != "" {
		s.addViolation(violation)
		return
	}
	s.executeCompaction(plan)
	s.checkInvariants()
}

func (s *simulator) state() contract.State {
	levels := make([][]contract.RunMeta, 2)
	for level := 0; level < 2; level++ {
		levels[level] = make([]contract.RunMeta, len(s.levels[level]))
		for index, run := range s.levels[level] {
			levels[level][index] = run.meta()
		}
	}
	return contract.State{
		Levels:      levels,
		DiskBytes:   s.diskBytes(),
		BudgetBytes: s.config.BudgetBytes,
		NowTick:     s.nowTick,
	}
}

func (s *simulator) findRun(id uint64) *run {
	for _, run := range s.levels[0] {
		if run.id == id {
			return run
		}
	}
	for _, run := range s.levels[1] {
		if run.id == id {
			return run
		}
	}
	return nil
}

func (s *simulator) validatePlan(plan contract.Plan) string {
	if len(plan.InputRunIDs) == 0 {
		return "compaction plan is empty"
	}
	if len(plan.InputRunIDs) > maxPlanInputs {
		return "compaction plan exceeds input run limit"
	}
	if plan.TargetLevel != 1 {
		return "compaction target level must be level-1"
	}

	seen := make(map[uint64]bool, len(plan.InputRunIDs))
	var selectedL0 []*run
	selectedL1 := make(map[uint64]*run)

	for _, id := range plan.InputRunIDs {
		if seen[id] {
			return "compaction plan contains duplicate run ids"
		}
		seen[id] = true

		run := s.findRun(id)
		if run == nil {
			return "compaction plan references a non-existent run"
		}
		switch run.level {
		case 0:
			selectedL0 = append(selectedL0, run)
		case 1:
			selectedL1[id] = run
		default:
			return "compaction plan references a run from an invalid level"
		}
	}

	if len(selectedL0) == 0 {
		return "compaction plan must include at least one level-0 run"
	}

	unionMin, unionMax, hasRange := unionRange(selectedL0)
	for _, l1 := range s.levels[1] {
		if !hasRange || !l1.overlapsRange(unionMin, unionMax) {
			continue
		}
		if _, ok := selectedL1[l1.id]; !ok {
			return "compaction plan is missing an overlapping level-1 run"
		}
	}

	if !hasRange {
		if len(selectedL1) != 0 {
			return "compaction plan includes a level-1 run but selected level-0 range is empty"
		}
		return ""
	}

	for _, l1 := range selectedL1 {
		if !l1.overlapsRange(unionMin, unionMax) {
			return "compaction plan includes a level-1 run that does not overlap the selected level-0 range"
		}
	}
	return ""
}

func (s *simulator) executeCompaction(plan contract.Plan) {
	inputs := make([]*run, 0, len(plan.InputRunIDs))
	for _, id := range plan.InputRunIDs {
		inputs = append(inputs, s.findRun(id))
	}

	output := newRun(s.nextRunID, plan.TargetLevel, s.nowTick, mergeEntries(inputs))
	s.nextRunID++
	for _, input := range inputs {
		s.removeRun(input)
	}
	s.levels[output.level] = append(s.levels[output.level], output)
	sortLevel1(s.levels[1])

	s.bytesRewritten = satAdd(s.bytesRewritten, output.bytes)
	s.compactionStalls++
}

func (s *simulator) removeRun(target *run) {
	level := s.levels[target.level]
	for index, run := range level {
		if run.id == target.id {
			s.levels[target.level] = append(level[:index], level[index+1:]...)
			return
		}
	}
}

func (s *simulator) lsmRead(key uint64) (entry, bool) {
	for index := len(s.memtable) - 1; index >= 0; index-- {
		if s.memtable[index].key == key {
			return s.memtable[index], true
		}
	}

	var best entry
	var bestCreated uint64
	var bestVersion uint64
	var bestRunID uint64
	found := false

	for _, run := range s.levels[0] {
		if !run.overlapsKey(key) {
			continue
		}
		s.logicalReadBytes = satAdd(s.logicalReadBytes, run.bytes)
		e, ok := run.findEntry(key)
		if !ok {
			continue
		}
		if !found || newerThan(run.created, e.version, run.id, bestCreated, bestVersion, bestRunID) {
			best = e
			bestCreated = run.created
			bestVersion = e.version
			bestRunID = run.id
			found = true
		}
	}
	if found {
		candidateCreated := bestCreated
		for _, run := range s.levels[1] {
			if !run.overlapsKey(key) || run.created < candidateCreated {
				continue
			}
			s.logicalReadBytes = satAdd(s.logicalReadBytes, run.bytes)
			e, ok := run.findEntry(key)
			if !ok {
				continue
			}
			if newerThan(run.created, e.version, run.id, bestCreated, bestVersion, bestRunID) {
				best = e
				bestCreated = run.created
				bestVersion = e.version
				bestRunID = run.id
			}
		}
		return best, true
	}

	for _, run := range s.levels[1] {
		if !run.overlapsKey(key) {
			continue
		}
		s.logicalReadBytes = satAdd(s.logicalReadBytes, run.bytes)
		if e, ok := run.findEntry(key); ok {
			return e, true
		}
	}
	return entry{}, false
}

// newerThan reports whether the candidate sorts before other under the merge
// ordering: CreatedTick descending, then Version descending, then run id
// descending.
func newerThan(created, version, runID, otherCreated, otherVersion, otherRunID uint64) bool {
	if created != otherCreated {
		return created > otherCreated
	}
	if version != otherVersion {
		return version > otherVersion
	}
	return runID > otherRunID
}

func (s *simulator) diskBytes() uint64 {
	total := uint64(0)
	for _, runs := range s.levels {
		for _, run := range runs {
			total = satAdd(total, run.bytes)
		}
	}
	return total
}

func (s *simulator) sampleDisk() {
	disk := s.diskBytes()
	if disk > s.peakDiskBytes {
		s.peakDiskBytes = disk
	}
}

func (s *simulator) checkInvariants() {
	if uint64(len(s.levels[0])) > s.config.MaxLevel0Runs {
		s.addViolation("level-0 run count exceeded hard limit")
	}
	if s.diskBytes() > s.config.BudgetBytes {
		s.addViolation("disk budget exceeded")
	}
}

func (s *simulator) call(fn func()) (ok bool) {
	defer func() {
		if recover() != nil {
			s.addViolation("contestant policy panicked")
			ok = false
		}
	}()
	fn()
	return true
}

func (s *simulator) addViolation(violation string) {
	s.violations = append(s.violations, violation)
}

func (s *simulator) result() Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(s.seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: WorkloadID,
		},
	}

	result.Metrics = Metrics{
		ReadAmplificationBPS: readAmplificationBPS(s.logicalReadBytes, s.requestedBytes),
		BytesRewritten:       s.bytesRewritten,
		CompactionStallTicks: s.compactionStalls,
		PeakDiskBytes:        s.peakDiskBytes,
	}
	result.Run.PeakMemoryBytes = s.peakDiskBytes

	if len(s.violations) > 0 {
		result.Violations = append(result.Violations, s.violations...)
		result.Verdict = "fail"
	}
	return result
}

func unionRange(runs []*run) (uint64, uint64, bool) {
	var minKey, maxKey uint64
	hasRange := false
	for _, run := range runs {
		if !run.hasKeys() {
			continue
		}
		if !hasRange {
			minKey, maxKey = run.minKey(), run.maxKey()
			hasRange = true
			continue
		}
		if run.minKey() < minKey {
			minKey = run.minKey()
		}
		if run.maxKey() > maxKey {
			maxKey = run.maxKey()
		}
	}
	return minKey, maxKey, hasRange
}

func mergeEntries(inputs []*run) []entry {
	type originEntry struct {
		e       entry
		created uint64
		runID   uint64
	}

	combined := make([]originEntry, 0)
	for _, input := range inputs {
		for _, e := range input.entries {
			combined = append(combined, originEntry{e: e, created: input.created, runID: input.id})
		}
	}

	sort.Slice(combined, func(i, j int) bool {
		a, b := combined[i], combined[j]
		if a.e.key != b.e.key {
			return a.e.key < b.e.key
		}
		if a.created != b.created {
			return a.created > b.created
		}
		if a.e.version != b.e.version {
			return a.e.version > b.e.version
		}
		return a.runID > b.runID
	})

	merged := make([]entry, 0, len(combined))
	for index := 0; index < len(combined); {
		key := combined[index].e.key
		merged = append(merged, combined[index].e)
		for index < len(combined) && combined[index].e.key == key {
			index++
		}
	}
	return merged
}

func sortLevel1(runs []*run) {
	sort.SliceStable(runs, func(i, j int) bool {
		if runs[i].hasKeys() != runs[j].hasKeys() {
			return !runs[i].hasKeys()
		}
		if runs[i].minKey() != runs[j].minKey() {
			return runs[i].minKey() < runs[j].minKey()
		}
		return runs[i].id < runs[j].id
	})
}

func sameEntry(a, b entry) bool {
	return a.key == b.key &&
		a.version == b.version &&
		a.tombstone == b.tombstone &&
		a.bytes == b.bytes
}

func readAmplificationBPS(readBytes, requestedBytes uint64) uint64 {
	if requestedBytes == 0 {
		return 0
	}
	if readBytes > ^uint64(0)/10000 {
		return ^uint64(0)
	}
	return readBytes * 10000 / requestedBytes
}

func satAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

func satMul(a, b uint64) uint64 {
	if a != 0 && b > ^uint64(0)/a {
		return ^uint64(0)
	}
	return a * b
}
