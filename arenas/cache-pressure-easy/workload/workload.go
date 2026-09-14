// Package workload generates the deterministic public request stream.
package workload

const (
	Read Kind = iota
	Write
	Delete
)

// Kind identifies an operation owned by the public harness.
type Kind uint8

// Operation is one deterministic request. Values are derived from key versions
// by the harness so workload fixtures remain compact.
type Operation struct {
	Kind Kind
	Key  uint64
}

// Config controls the public workload without exposing ranked distributions.
type Config struct {
	Seed       uint64
	Operations int
	HotKeys    uint64
	ScanKeys   uint64
}

// Generate creates hot-set, scan, and phase-change segments from a 64-bit seed.
func Generate(config Config) []Operation {
	if config.Operations <= 0 {
		return nil
	}
	if config.HotKeys == 0 {
		config.HotKeys = 16
	}
	if config.ScanKeys == 0 {
		config.ScanKeys = 96
	}

	random := splitMix64{state: config.Seed}
	operations := make([]Operation, 0, config.Operations)
	firstEnd := config.Operations * 4 / 10
	secondEnd := config.Operations * 7 / 10
	secondHotBase := config.HotKeys + config.ScanKeys + 1

	for index := 0; index < config.Operations; index++ {
		switch {
		case index < firstEnd:
			key := random.next() % config.HotKeys
			operations = append(operations, mixedOperation(&random, key))
		case index < secondEnd:
			key := config.HotKeys + uint64(index-firstEnd)%config.ScanKeys
			operations = append(operations, Operation{Kind: Read, Key: key})
		default:
			key := secondHotBase + random.next()%config.HotKeys
			operations = append(operations, mixedOperation(&random, key))
		}
	}

	return operations
}

func mixedOperation(random *splitMix64, key uint64) Operation {
	switch random.next() % 20 {
	case 0:
		return Operation{Kind: Delete, Key: key}
	case 1, 2, 3:
		return Operation{Kind: Write, Key: key}
	default:
		return Operation{Kind: Read, Key: key}
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
