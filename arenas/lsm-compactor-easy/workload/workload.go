// Package workload generates the deterministic public operation stream for
// the lsm-compactor-easy arena. The harness owns this stream and executes it
// against the immutable storage engine.
package workload

// Kind identifies an operation owned by the public harness.
type Kind uint8

const (
	// Write inserts or overwrites one key and may flush the memtable.
	Write Kind = iota
	// Read performs one point lookup.
	Read
	// Delete writes a tombstone for one key and may flush the memtable.
	Delete
	// Scan performs a bounded forward scan.
	Scan
	// Snapshot verifies every live reference key.
	Snapshot
)

// Operation is one deterministic request. Values are derived by the harness so
// workload fixtures remain compact.
type Operation struct {
	Kind   Kind
	Key    uint64
	Length int
}

// Config controls the public workload without exposing ranked distributions.
type Config struct {
	Seed       uint64
	Operations int
	KeySpace   uint64
	ScanLength int
}

// Generate creates write-heavy, mixed, and read-heavy phases from a 64-bit
// seed. Snapshot operations are emitted periodically so the harness exercises
// full-state verification without making the smoke run slow.
func Generate(config Config) []Operation {
	if config.Operations <= 0 {
		return nil
	}
	if config.KeySpace == 0 {
		config.KeySpace = 256
	}
	if config.ScanLength <= 0 {
		config.ScanLength = 16
	}

	random := splitMix64{state: config.Seed}
	operations := make([]Operation, 0, config.Operations)
	firstEnd := config.Operations * 30 / 100
	secondEnd := config.Operations * 70 / 100

	for index := 0; index < config.Operations; index++ {
		if index > 0 && index%500 == 0 {
			operations = append(operations, Operation{Kind: Snapshot})
			continue
		}

		switch {
		case index < firstEnd:
			operations = append(operations, phaseOperation(&random, config, 70, 20, 10, 0))
		case index < secondEnd:
			operations = append(operations, phaseOperation(&random, config, 35, 45, 15, 5))
		default:
			operations = append(operations, phaseOperation(&random, config, 15, 65, 10, 10))
		}
	}

	return operations
}

// phaseOperation picks one operation according to the given write/read/delete/
// scan percentages. Percentages are integers that should sum to at most 100;
// any remainder is treated as a scan.
func phaseOperation(random *splitMix64, config Config, writePct, readPct, deletePct, scanPct int) Operation {
	roll := int(random.next() % 100)
	switch {
	case roll < writePct:
		return Operation{Kind: Write, Key: random.next() % config.KeySpace}
	case roll < writePct+readPct:
		return Operation{Kind: Read, Key: random.next() % config.KeySpace}
	case roll < writePct+readPct+deletePct:
		return Operation{Kind: Delete, Key: random.next() % config.KeySpace}
	default:
		start := random.next() % config.KeySpace
		length := config.ScanLength
		if uint64(length) > config.KeySpace-start {
			length = int(config.KeySpace - start)
		}
		return Operation{Kind: Scan, Key: start, Length: length}
	}
}

type splitMix64 struct {
	state uint64
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	value := r.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
