package workload

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/roughstack/challenges/challenges/chunk-store-easy/contract"
)

func TestGenerateIsDeterministicAndCoversKinds(t *testing.T) {
	config := Config{Seed: ^uint64(0), Blobs: 24, MaxBlobBytes: DefaultMaxBlobBytes}
	first := Generate(config)
	second := Generate(config)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same uint64 seed produced different blobs")
	}

	seen := map[Kind]bool{}
	for _, blob := range first {
		seen[blob.Kind] = true
		if uint64(len(blob.Data)) > contract.MaxBlobBytes {
			t.Fatalf("blob %d exceeds MaxBlobBytes", blob.ID)
		}
	}
	for _, kind := range []Kind{KindEmpty, KindOneByte, KindCompressible, KindIncompressible, KindMixed} {
		if !seen[kind] {
			t.Fatalf("kind %d was not generated", kind)
		}
	}
}

func TestGenerateIncludesDeterministicDuplicateKeys(t *testing.T) {
	blobs := Generate(Config{Seed: 7, Blobs: 14, MaxBlobBytes: DefaultMaxBlobBytes})

	keys := map[[2]uint64]int{}
	for _, blob := range blobs {
		keys[[2]uint64{blob.ID, blob.Version}]++
	}
	duplicates := 0
	for _, count := range keys {
		if count > 1 {
			duplicates++
		}
	}
	if duplicates == 0 {
		t.Fatal("workload did not include duplicate id/version pairs")
	}
}

func TestCompressibleAndIncompressibleBlobsHaveExpectedShape(t *testing.T) {
	blobs := Generate(Config{Seed: 11, Blobs: 20, MaxBlobBytes: DefaultMaxBlobBytes})
	var compressible, incompressible bool
	for _, blob := range blobs {
		switch blob.Kind {
		case KindCompressible:
			compressible = true
			if len(blob.Data) <= int(contract.BlockSize) {
				t.Fatal("compressible blob is not larger than one block")
			}
			first := blob.Data[0]
			for _, b := range blob.Data {
				if b != first {
					t.Fatal("compressible blob is not repeated bytes")
				}
			}
		case KindIncompressible:
			incompressible = true
			if len(blob.Data) <= int(contract.BlockSize) {
				t.Fatal("incompressible blob is not larger than one block")
			}
			if bytes.Equal(blob.Data, bytes.Repeat([]byte{blob.Data[0]}, len(blob.Data))) {
				t.Fatal("incompressible blob is entirely repeated bytes")
			}
		}
	}
	if !compressible || !incompressible {
		t.Fatal("workload did not include both compressible and incompressible multi-block blobs")
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if blobs := Generate(Config{}); blobs != nil {
		t.Fatalf("empty config produced %d blobs", len(blobs))
	}
}

func TestGenerateClampsMaxBlobBytesToContractBound(t *testing.T) {
	blobs := Generate(Config{Seed: 3, Blobs: 8, MaxBlobBytes: int(contract.MaxBlobBytes) + 1000})
	for _, blob := range blobs {
		if uint64(len(blob.Data)) > contract.MaxBlobBytes {
			t.Fatalf("blob %d exceeded MaxBlobBytes", blob.ID)
		}
	}
}
