// Package starter is the learner-facing scaffold for a framed blob store.
package starter

import (
	"io"

	"github.com/roughstack/challenges/challenges/chunk-store-easy/contract"
)

type store struct {
	device      contract.Device
	memoryLimit uint64
	// TODO: keep only a bounded in-memory index.
}

func OpenStore(_ string, device contract.Device, memoryLimit uint64) (contract.Store, error) {
	return &store{device: device, memoryLimit: memoryLimit}, nil
}

func (s *store) Put(id, version uint64, data io.Reader, size uint64) error {
	panic("TODO: encode and commit a verifiable frame")
}

func (s *store) ReadAt(id, version uint64, p []byte, off int64) (int, error) {
	panic("TODO: find, validate, and slice the requested blob")
}

func (s *store) Delete(id, version uint64) error {
	panic("TODO: mark one version deleted")
}

func (s *store) Compact() error {
	panic("TODO: reclaim space without changing visible bytes")
}

func (s *store) Close() error {
	panic("TODO: release the store")
}

func (s *store) IndexBytes() uint64 {
	panic("TODO: report bounded index memory")
}
