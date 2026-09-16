package workload

import (
	"bytes"
	"reflect"
	"testing"
)

func TestGenerateIsDeterministicAndBounded(t *testing.T) {
	config := Config{Seed: ^uint64(0), Records: 200, MaxPayload: 512}
	first := Generate(config)
	second := Generate(config)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed produced different records")
	}

	if len(first) != config.Records {
		t.Fatalf("record count = %d, want %d", len(first), config.Records)
	}
	for index, record := range first {
		if record.LSN != uint64(index+1) {
			t.Fatalf("record %d has LSN %d, want %d", index, record.LSN, index+1)
		}
		if len(record.Payload) < 1 || len(record.Payload) > config.MaxPayload {
			t.Fatalf("record %d has out-of-range payload size %d", record.LSN, len(record.Payload))
		}
	}
}

func TestPayloadIsDeterministic(t *testing.T) {
	first := Payload(7, 42, 96)
	second := Payload(7, 42, 96)
	if !bytes.Equal(first, second) {
		t.Fatal("same seed/lsn/size produced different payload bytes")
	}
	if bytes.Equal(first, Payload(8, 42, 96)) {
		t.Fatal("different seed produced identical payload bytes")
	}
	if bytes.Equal(first, Payload(7, 43, 96)) {
		t.Fatal("different LSN produced identical payload bytes")
	}
	if len(Payload(7, 42, 0)) != 0 {
		t.Fatal("zero-size payload was not empty")
	}
}

func TestGenerateHandlesEmptyConfig(t *testing.T) {
	if records := Generate(Config{}); records != nil {
		t.Fatalf("empty config produced %d records", len(records))
	}
}
