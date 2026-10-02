package runlog

import (
	"encoding/hex"
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
	const canonical = "a664747970656b7461736b2e6c6f672e763166736f757263656673746465727267617474656d707401676d6573736167656568656c6c6f69616374696f6e5f6964782430303030303030302d303030302d303030302d303030302d3030303030303030303030306974696d657374616d7074323032362d30392d33305430303a30303a30305a"
	if got := hex.EncodeToString(encoded); got != canonical {
		t.Fatalf("canonical encoding = %s", got)
	}
	noncanonical := append([]byte{0xbf}, encoded[1:]...)
	noncanonical = append(noncanonical, 0xff)
	if _, err := DecodeTaskLog(noncanonical); err == nil {
		t.Fatal("noncanonical encoding was accepted")
	}
}

func TestTaskLogRejectsUnknownEvent(t *testing.T) {
	if _, err := DecodeTaskLog([]byte{0xa1, 0x64, 0x74, 0x79, 0x70, 0x65, 0x65, 0x6f, 0x74, 0x68, 0x65, 0x72}); err == nil {
		t.Fatal("expected unknown event rejection")
	}
}
