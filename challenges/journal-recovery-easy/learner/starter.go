// Package starter is the learner-facing scaffold for a crash-safe journal.
package starter

import "github.com/roughstack/challenges/challenges/journal-recovery-easy/contract"

type journal struct {
	device contract.Device
	// TODO: track the next LSN, durable prefix, and closed state.
}

func OpenJournal(_ string, device contract.Device) (contract.Journal, error) {
	return &journal{device: device}, nil
}

func (j *journal) Append(record contract.Record) (uint64, error) {
	panic("TODO: encode a bounded, checksummed frame")
}

func (j *journal) Sync(upto uint64) error {
	panic("TODO: persist a known prefix")
}

func (j *journal) Recover(apply func(contract.Record) error) (contract.RecoveryInfo, error) {
	panic("TODO: replay the valid durable prefix")
}

func (j *journal) Close() error {
	panic("TODO: close idempotently")
}
