package runlog

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTaskLogRoundTripCanonical(t *testing.T) {
	event := TaskLogEvent{
		Type: "task.log.v1", Source: "stderr", Message: "hello",
		Timestamp: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		ActionID:  uuid.Nil.String(), Attempt: 1,
	}
	encoded, err := EncodeTaskLog(event)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTaskLog(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != event {
		t.Fatalf("decoded event differs: %#v", decoded)
	}
	if string(encoded) != string(mustEncodeTaskLog(t, event)) {
		t.Fatal("encoding is not deterministic")
	}
}

func TestTaskLogRejectsUnknownEvent(t *testing.T) {
	if _, err := DecodeTaskLog([]byte{0xa1, 0x64, 0x74, 0x79, 0x70, 0x65, 0x65, 0x6f, 0x74, 0x68, 0x65, 0x72}); err == nil {
		t.Fatal("expected unknown event rejection")
	}
}

func mustEncodeTaskLog(t *testing.T, event TaskLogEvent) []byte {
	t.Helper()
	encoded, err := EncodeTaskLog(event)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
