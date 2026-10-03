// Package harness owns public execution, the deterministic device, fault
// injection, accounting, correctness gates, and result construction for the
// chunk-store-easy arena. The contestant surface is contract.Store only.
package harness

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/roughstack/challenges/challenges/chunk-store-easy/contract"
	"github.com/roughstack/challenges/challenges/chunk-store-easy/workload"
)

const (
	// ArenaID is the stable public identifier for this variant.
	ArenaID = "chunk-store-easy"
	// ArenaVersion is the immutable public version for this variant.
	ArenaVersion = "1.0.0"
	// WorkloadID identifies the deterministic public smoke workload.
	WorkloadID = "public-smoke-v1"

	// blockReadSlack is the tolerated device-byte overhead for a ReadAt call.
	// Correct stores read each touched block header and payload exactly once;
	// this slack allows a store to fetch a header and payload in separate calls.
	blockReadSlack = 64

	// maxCorruptReadAllocBytes bounds the heap a store may allocate while
	// rejecting a corrupt block. A correct implementation reads at most a block
	// header/payload and decodes at most BlockSize bytes.
	maxCorruptReadAllocBytes = 1 << 20

	// maxViolations bounds recorded violations before a run stops early.
	maxViolations = 16
)

// FaultProfile selects a deterministic public corruption applied after the
// write phase and before the read phase.
type FaultProfile string

const (
	FaultClean              FaultProfile = "clean"
	FaultChecksumCorruption FaultProfile = "checksum-corruption"
	FaultCorruptLength      FaultProfile = "corrupt-length"
)

// Metrics are deterministic logical measurements from one workload run.
type Metrics struct {
	StoredBytes            uint64 `json:"stored_bytes"`
	RandomReadLogicalBytes uint64 `json:"random_read_logical_bytes"`
	WriteCPUWork           uint64 `json:"write_cpu_work"`
	ReadCPUWork            uint64 `json:"read_cpu_work"`
	IndexMemoryBytes       uint64 `json:"index_memory_bytes"`
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
	Blobs        int
	MaxBlobBytes int
	ReadsPerBlob int
	TinyReads    int
	MemoryLimit  uint64
	DeleteEvery  int
	Fault        FaultProfile
}

// DefaultPublicConfig returns the fast public smoke workload.
func DefaultPublicConfig() PublicConfig {
	return PublicConfig{
		Blobs:        24,
		MaxBlobBytes: workload.DefaultMaxBlobBytes,
		ReadsPerBlob: 3,
		TinyReads:    120,
		MemoryLimit:  1 << 20,
		DeleteEvery:  7,
		Fault:        FaultClean,
	}
}

// Evaluate runs the deterministic public workload against a contestant store.
func Evaluate(seed uint64, config PublicConfig, factory contract.Factory) Result {
	normalize(&config)
	return run(seed, config, factory, &memDevice{})
}

type blobKey struct {
	id      uint64
	version uint64
}

type blockRecord struct {
	offset uint64
	rawLen uint32
	encLen uint32
}

type blobRecord struct {
	id      uint64
	version uint64
	size    uint64
	blocks  []blockRecord
}

type runner struct {
	result      *Result
	device      *memDevice
	store       contract.Store
	memoryLimit uint64
	ref         map[blobKey]blobRecord
	corruptKeys map[blobKey]bool
	panicked    bool

	writeCPUWork     uint64
	readDeviceBytes  uint64
	readLogicalBytes uint64
	peakIndex        uint64
	storedAfterWrite uint64
}

func run(seed uint64, config PublicConfig, factory contract.Factory, device *memDevice) Result {
	result := Result{
		ProtocolVersion: 1,
		ArenaID:         ArenaID,
		ArenaVersion:    ArenaVersion,
		Seed:            strconv.FormatUint(seed, 10),
		Verdict:         "pass",
		Score:           0,
		Violations:      []string{},
		Run: RunInfo{
			WorkloadID: WorkloadID,
		},
	}

	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)

	r := &runner{
		result:      &result,
		device:      device,
		memoryLimit: config.MemoryLimit,
		ref:         make(map[blobKey]blobRecord),
		corruptKeys: make(map[blobKey]bool),
	}

	if factory == nil {
		r.violate("store factory is nil")
		return r.finish()
	}

	r.store = r.openStore(factory)
	if r.store == nil {
		return r.finish()
	}
	r.sampleIndex()

	blobs := workload.Generate(workload.Config{
		Seed:         seed,
		Blobs:        config.Blobs,
		MaxBlobBytes: config.MaxBlobBytes,
	})

	model := make(map[blobKey][]byte)
	var putOrder []blobKey

	// Write phase.
	for _, blob := range blobs {
		if r.panicked || len(result.Violations) >= maxViolations {
			break
		}
		key := blobKey{id: blob.ID, version: blob.Version}
		_, seen := model[key]
		beforeWrites := r.device.writeBytes

		var putErr error
		_, ok := r.call(func() {
			putErr = r.store.Put(blob.ID, blob.Version, bytes.NewReader(blob.Data), uint64(len(blob.Data)))
		})
		r.writeCPUWork = satAdd(r.writeCPUWork, uint64(len(blob.Data)))
		r.writeCPUWork = satAdd(r.writeCPUWork, r.device.writeBytes-beforeWrites)
		if !ok {
			break
		}

		switch {
		case putErr == nil && seen:
			r.violate("duplicate Put was accepted")
		case putErr == nil:
			model[key] = append([]byte(nil), blob.Data...)
			putOrder = append(putOrder, key)
		case seen && errors.Is(putErr, contract.ErrDuplicate):
			// Expected duplicate rejection.
		default:
			r.violate("Put returned an unexpected error: " + putErr.Error())
		}
		r.sampleIndex()
	}

	if !r.panicked {
		var scanErr error
		r.ref, scanErr = scanDevice(r.device)
		if scanErr != nil {
			r.violate("device does not contain valid blob records: " + scanErr.Error())
		} else {
			// stored_bytes is defined as the device bytes occupied after the
			// write phase, so capture it before any fault injection or later
			// delete/close work can change the device.
			r.storedAfterWrite = uint64(len(r.device.data))
			r.applyFault(config.Fault, putOrder)
		}
	}

	// Read phase.
	r.runReads(putOrder, model, config)

	// The easy tier does not reclaim device bytes, so Compact must be a no-op
	// while the store is open. Exercise it here rather than only after Close.
	beforeCompact := uint64(len(r.device.data))
	var compactErr error
	if _, ok := r.call(func() { compactErr = r.store.Compact() }); ok {
		if compactErr != nil {
			r.violate("Compact returned an unexpected error: " + compactErr.Error())
		}
		if uint64(len(r.device.data)) != beforeCompact {
			r.violate("Compact reclaimed device bytes")
		}
	}

	// Delete phase: every DeleteEvery-th successfully stored blob is removed,
	// then reads for it must return ErrNotFound.
	for index, key := range putOrder {
		if r.panicked || len(result.Violations) >= maxViolations {
			break
		}
		if config.DeleteEvery <= 0 || index%config.DeleteEvery != 0 {
			continue
		}
		beforeDelete := uint64(len(r.device.data))
		var delErr error
		if _, ok := r.call(func() { delErr = r.store.Delete(key.id, key.version) }); !ok {
			break
		}
		if uint64(len(r.device.data)) < beforeDelete {
			r.violate("Delete reclaimed device bytes")
		}
		if delErr != nil {
			r.violate("Delete returned an unexpected error: " + delErr.Error())
			continue
		}
		delete(model, key)
		delete(r.ref, key)
		delete(r.corruptKeys, key)

		p := make([]byte, 1)
		var n int
		var readErr error
		before := r.device.readBytes
		if _, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, p, 0) }); ok {
			r.readDeviceBytes = satAdd(r.readDeviceBytes, r.device.readBytes-before)
			if n != 0 || !errors.Is(readErr, contract.ErrNotFound) {
				r.violate("ReadAt after Delete did not return ErrNotFound")
			}
		}
	}

	r.closeChecks()
	return r.finish()
}

func (r *runner) openStore(factory contract.Factory) contract.Store {
	var store contract.Store
	var openErr error
	if _, ok := r.call(func() {
		store, openErr = factory("", r.device, r.memoryLimit)
	}); !ok {
		return nil
	}
	if openErr != nil {
		r.violate("OpenStore failed: " + openErr.Error())
		return nil
	}
	if store == nil {
		r.violate("OpenStore returned a nil store")
		return nil
	}
	return store
}

func (r *runner) runReads(putOrder []blobKey, model map[blobKey][]byte, config PublicConfig) {
	// Full and boundary reads for every successfully stored blob. The full read
	// also guarantees that any injected corruption is actually observed.
	for _, key := range putOrder {
		if r.panicked || len(r.result.Violations) >= maxViolations {
			return
		}
		data := model[key]
		if r.corruptKeys[key] {
			r.readCorrupt(key, len(data))
		} else if len(data) == 0 {
			r.readEmpty(key)
		} else {
			r.readExact(key, data, 0, len(data))
		}

		if r.panicked || len(r.result.Violations) >= maxViolations {
			return
		}
		r.readOutOfRange(key, int64(len(data)), 1)
		r.readNegative(key)
	}

	cleanKeys := make([]blobKey, 0, len(putOrder))
	for _, key := range putOrder {
		if !r.corruptKeys[key] {
			cleanKeys = append(cleanKeys, key)
		}
	}
	if len(cleanKeys) == 0 {
		return
	}

	random := splitMix64{state: r.resultSeed()}
	readOps := config.ReadsPerBlob*len(putOrder) + config.TinyReads
	for index := 0; index < readOps; index++ {
		if r.panicked || len(r.result.Violations) >= maxViolations {
			return
		}
		key := cleanKeys[int(random.next()%uint64(len(cleanKeys)))]
		data := model[key]
		if len(data) == 0 {
			r.readEmpty(key)
			continue
		}

		length := 1
		if index >= config.TinyReads {
			maxLength := len(data)
			if maxLength > 256 {
				maxLength = 256
			}
			length = 1 + int(random.next()%uint64(maxLength))
		}
		maxOff := len(data) - length
		off := int64(random.next() % uint64(maxOff+1))
		r.readExact(key, data, off, length)
	}
}

func (r *runner) readExact(key blobKey, data []byte, off int64, length int) {
	p := make([]byte, length)
	before := r.device.readBytes
	r.readLogicalBytes = satAdd(r.readLogicalBytes, uint64(length))

	var n int
	var readErr error
	if _, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, p, off) }); !ok {
		return
	}
	after := r.device.readBytes
	r.readDeviceBytes = satAdd(r.readDeviceBytes, after-before)

	if uint64(off) > uint64(len(data)) || uint64(length) > uint64(len(data))-uint64(off) {
		r.violate("exact read fixture is out of range")
		return
	}
	want := data[off : off+int64(length)]
	if readErr != nil {
		r.violate("exact read returned an unexpected error: " + readErr.Error())
		return
	}
	if n != len(want) {
		r.violate("exact read returned the wrong number of bytes")
		return
	}
	if !bytes.Equal(p[:n], want) {
		r.violate("exact read returned the wrong data")
	}

	if rec, ok := r.ref[key]; ok {
		expectedDevice := expectedReadBytes(rec, off, length)
		if after-before > expectedDevice+blockReadSlack {
			r.violate("read decoded unrelated blobs")
		}
	}
}

func (r *runner) readEmpty(key blobKey) {
	before := r.device.readBytes
	var n int
	var readErr error
	if _, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, nil, 0) }); !ok {
		return
	}
	after := r.device.readBytes
	r.readDeviceBytes = satAdd(r.readDeviceBytes, after-before)
	if n != 0 || readErr != nil {
		r.violate("empty read did not return (0, nil)")
	}
}

func (r *runner) readCorrupt(key blobKey, size int) {
	length := size
	if length <= 0 {
		length = 1
	}
	p := make([]byte, length)
	before := r.device.readBytes
	r.readLogicalBytes = satAdd(r.readLogicalBytes, uint64(length))

	var n int
	var readErr error
	alloc, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, p, 0) })
	if !ok {
		return
	}
	after := r.device.readBytes
	r.readDeviceBytes = satAdd(r.readDeviceBytes, after-before)

	if n != 0 {
		r.violate("read of a corrupt blob returned bytes")
	}
	if readErr == nil || (!errors.Is(readErr, contract.ErrCorrupt) && !errors.Is(readErr, contract.ErrLengthBomb)) {
		r.violate("read of a corrupt blob did not return a corruption error")
	}
	if alloc > maxCorruptReadAllocBytes {
		r.violate("read over-allocated on corrupt input")
	}
	if after-before > contract.BlockHeaderSize+contract.BlockSize+blockReadSlack {
		r.violate("corrupt read touched unrelated device bytes")
	}
}

func (r *runner) readOutOfRange(key blobKey, off int64, length int) {
	p := make([]byte, length)
	before := r.device.readBytes
	r.readLogicalBytes = satAdd(r.readLogicalBytes, uint64(length))

	var n int
	var readErr error
	if _, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, p, off) }); !ok {
		return
	}
	after := r.device.readBytes
	r.readDeviceBytes = satAdd(r.readDeviceBytes, after-before)
	if n != 0 || !errors.Is(readErr, io.EOF) {
		r.violate("out-of-range read did not return io.EOF")
	}
}

func (r *runner) readNegative(key blobKey) {
	p := make([]byte, 1)
	before := r.device.readBytes
	r.readLogicalBytes = satAdd(r.readLogicalBytes, 1)

	var n int
	var readErr error
	if _, ok := r.call(func() { n, readErr = r.store.ReadAt(key.id, key.version, p, -1) }); !ok {
		return
	}
	after := r.device.readBytes
	r.readDeviceBytes = satAdd(r.readDeviceBytes, after-before)
	if n != 0 || !errors.Is(readErr, contract.ErrRange) {
		r.violate("negative read did not return ErrRange")
	}
}

func (r *runner) closeChecks() {
	if r.panicked || r.store == nil {
		return
	}

	beforeClose := uint64(len(r.device.data))
	var err error
	if _, ok := r.call(func() { err = r.store.Close() }); ok && err != nil {
		r.violate("first Close failed: " + err.Error())
	}
	if uint64(len(r.device.data)) != beforeClose {
		r.violate("Close reclaimed device bytes")
	}
	if _, ok := r.call(func() { err = r.store.Close() }); ok && err != nil {
		r.violate("second Close was not idempotent")
	}

	if _, ok := r.call(func() { err = r.store.Put(1, 1, bytes.NewReader([]byte{1}), 1) }); ok && !errors.Is(err, contract.ErrClosed) {
		r.violate("Put after Close did not return ErrClosed")
	}
	if _, ok := r.call(func() {
		_, err = r.store.ReadAt(1, 1, make([]byte, 1), 0)
	}); ok && !errors.Is(err, contract.ErrClosed) {
		r.violate("ReadAt after Close did not return ErrClosed")
	}
	if _, ok := r.call(func() { err = r.store.Delete(1, 1) }); ok && !errors.Is(err, contract.ErrClosed) {
		r.violate("Delete after Close did not return ErrClosed")
	}
	if _, ok := r.call(func() { err = r.store.Compact() }); ok && !errors.Is(err, contract.ErrClosed) {
		r.violate("Compact after Close did not return ErrClosed")
	}
}

func (r *runner) sampleIndex() {
	if r.store == nil {
		return
	}
	var indexBytes uint64
	if _, ok := r.call(func() { indexBytes = r.store.IndexBytes() }); !ok {
		return
	}
	if indexBytes > r.peakIndex {
		r.peakIndex = indexBytes
	}
	if indexBytes > r.memoryLimit {
		r.violate("index bytes exceed memory limit")
	}
}

func (r *runner) applyFault(fault FaultProfile, putOrder []blobKey) {
	if fault == "" || fault == FaultClean {
		return
	}
	for _, key := range putOrder {
		rec, ok := r.ref[key]
		if !ok || len(rec.blocks) == 0 {
			continue
		}
		r.applyFaultToRecord(rec, fault)
		r.corruptKeys[key] = true
		return
	}
}

func (r *runner) applyFaultToRecord(rec blobRecord, fault FaultProfile) {
	if len(rec.blocks) == 0 {
		return
	}
	first := rec.blocks[0]
	switch fault {
	case FaultChecksumCorruption:
		payloadOffset := first.offset + contract.BlockHeaderSize
		if payloadOffset < uint64(len(r.device.data)) {
			r.device.data[payloadOffset] ^= 0x01
		}
	case FaultCorruptLength:
		headerOffset := first.offset
		if headerOffset+8 <= uint64(len(r.device.data)) {
			binary.BigEndian.PutUint32(r.device.data[headerOffset+4:headerOffset+8], uint32(contract.MaxBlockPayloadBytes)+1)
		}
	}
}

func (r *runner) call(fn func()) (alloc uint64, ok bool) {
	if r.panicked {
		return 0, false
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ok = true
	defer func() {
		if recover() != nil {
			r.panicked = true
			r.violate("contestant store panicked")
			ok = false
		}
		runtime.ReadMemStats(&after)
		alloc = after.TotalAlloc - before.TotalAlloc
	}()
	fn()
	return alloc, true
}

func (r *runner) violate(message string) {
	if len(r.result.Violations) < maxViolations {
		r.result.Violations = append(r.result.Violations, message)
		r.result.Verdict = "fail"
	}
}

func (r *runner) finish() Result {
	r.result.Metrics.StoredBytes = r.storedAfterWrite
	r.result.Metrics.RandomReadLogicalBytes = r.readDeviceBytes
	r.result.Metrics.WriteCPUWork = r.writeCPUWork
	r.result.Metrics.ReadCPUWork = satAdd(r.readLogicalBytes, r.readDeviceBytes)
	r.result.Metrics.IndexMemoryBytes = r.peakIndex
	r.result.Run.PeakMemoryBytes = r.peakIndex
	if len(r.result.Violations) > 0 {
		r.result.Verdict = "fail"
	}
	return *r.result
}

func (r *runner) resultSeed() uint64 {
	seed, err := strconv.ParseUint(r.result.Seed, 10, 64)
	if err != nil {
		return 0
	}
	return seed
}

func normalize(config *PublicConfig) {
	if config.Blobs <= 0 {
		config.Blobs = DefaultPublicConfig().Blobs
	}
	if config.MaxBlobBytes <= 0 {
		config.MaxBlobBytes = DefaultPublicConfig().MaxBlobBytes
	}
	if config.ReadsPerBlob <= 0 {
		config.ReadsPerBlob = DefaultPublicConfig().ReadsPerBlob
	}
	if config.TinyReads < 0 {
		config.TinyReads = 0
	}
	if config.MemoryLimit == 0 {
		config.MemoryLimit = DefaultPublicConfig().MemoryLimit
	}
	if config.DeleteEvery <= 0 {
		config.DeleteEvery = 1
	}
	// The harness owns this config, so an unrecognized fault profile is a local
	// typo: degrade to a clean run rather than fabricating corruption
	// expectations for blobs that were never mutated.
	switch config.Fault {
	case "", FaultClean, FaultChecksumCorruption, FaultCorruptLength:
		if config.Fault == "" {
			config.Fault = FaultClean
		}
	default:
		config.Fault = FaultClean
	}
}

func expectedReadBytes(rec blobRecord, off int64, length int) uint64 {
	if length <= 0 || off < 0 || uint64(off) >= rec.size || len(rec.blocks) == 0 {
		return 0
	}
	end := uint64(off) + uint64(length)
	if end > rec.size {
		end = rec.size
	}
	first := uint64(off) / contract.BlockSize
	last := (end - 1) / contract.BlockSize
	if last >= uint64(len(rec.blocks)) {
		last = uint64(len(rec.blocks) - 1)
	}

	var total uint64
	for index := first; index <= last; index++ {
		total += contract.BlockHeaderSize + uint64(rec.blocks[index].encLen)
	}
	return total
}

// scanDevice reconstructs the public on-device index used as the harness's
// ground-truth model. It mirrors the documented contract exactly.
func scanDevice(device *memDevice) (map[blobKey]blobRecord, error) {
	ref := make(map[blobKey]blobRecord)
	data := device.data
	if len(data) == 0 {
		return ref, nil
	}

	offset := uint64(0)
	for offset < uint64(len(data)) {
		if uint64(len(data))-offset < contract.BlobHeaderSize {
			return nil, contract.ErrCorrupt
		}
		header := data[offset : offset+contract.BlobHeaderSize]
		id, version, size, blockCount, ok := contract.ParseBlobHeader(header)
		if !ok || !contract.VerifyBlobHeader(header) {
			return nil, contract.ErrCorrupt
		}
		if err := contract.ValidateBlobHeader(size, blockCount); err != nil {
			return nil, err
		}
		key := blobKey{id: id, version: version}
		if _, exists := ref[key]; exists {
			return nil, contract.ErrCorrupt
		}

		rec := blobRecord{id: id, version: version, size: size, blocks: make([]blockRecord, 0, blockCount)}
		offset += contract.BlobHeaderSize
		remaining := size
		for index := uint64(0); index < blockCount; index++ {
			if uint64(len(data))-offset < contract.BlockHeaderSize {
				return nil, contract.ErrCorrupt
			}
			blockHeader := data[offset : offset+contract.BlockHeaderSize]
			rawLen, encLen, ok := contract.ParseBlockHeader(blockHeader)
			if !ok {
				return nil, contract.ErrCorrupt
			}
			if err := contract.ValidateBlockRecord(rawLen, encLen); err != nil {
				return nil, err
			}
			// The documented layout is fixed-size: every non-final block holds
			// exactly BlockSize raw bytes and the final block holds the
			// remainder. Reject non-uniform framing even when checksums and the
			// length-sum are internally consistent.
			expectedRaw := contract.BlockSize
			if index == blockCount-1 {
				expectedRaw = size - (blockCount-1)*contract.BlockSize
			}
			if uint64(rawLen) != expectedRaw {
				return nil, contract.ErrCorrupt
			}
			if uint64(encLen) > uint64(len(data))-(offset+contract.BlockHeaderSize) {
				return nil, contract.ErrCorrupt
			}
			payload := data[offset+contract.BlockHeaderSize : offset+contract.BlockHeaderSize+uint64(encLen)]
			if !contract.VerifyBlockRecord(blockHeader, payload) {
				return nil, contract.ErrCorrupt
			}
			if uint64(rawLen) > remaining {
				return nil, contract.ErrCorrupt
			}
			remaining -= uint64(rawLen)
			rec.blocks = append(rec.blocks, blockRecord{offset: offset, rawLen: rawLen, encLen: encLen})
			offset += contract.BlockHeaderSize + uint64(encLen)
		}
		if remaining != 0 {
			return nil, contract.ErrCorrupt
		}
		ref[key] = rec
	}
	return ref, nil
}

// memDevice is the harness-owned deterministic device with explicit accounting.
type memDevice struct {
	data []byte

	reads      uint64
	readBytes  uint64
	writes     uint64
	writeBytes uint64
	syncs      uint64
	truncates  uint64
}

func (d *memDevice) Size() (uint64, error) { return uint64(len(d.data)), nil }

func (d *memDevice) Read(offset uint64, length uint64) ([]byte, error) {
	if offset > uint64(len(d.data)) || length > uint64(len(d.data))-offset {
		return nil, errors.New("device: read out of range")
	}
	d.reads++
	d.readBytes += length
	return append([]byte(nil), d.data[offset:offset+length]...), nil
}

func (d *memDevice) Write(offset uint64, p []byte) error {
	end := offset + uint64(len(p))
	if end > uint64(len(d.data)) {
		grown := make([]byte, end)
		copy(grown, d.data)
		d.data = grown
	}
	copy(d.data[offset:], p)
	d.writes++
	d.writeBytes += uint64(len(p))
	return nil
}

func (d *memDevice) Sync() error {
	d.syncs++
	return nil
}

func (d *memDevice) Truncate(size uint64) error {
	if size > uint64(len(d.data)) {
		grown := make([]byte, size)
		copy(grown, d.data)
		d.data = grown
	} else {
		d.data = d.data[:size]
	}
	d.truncates++
	return nil
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

func satAdd(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}
