package runlog

import (
	"context"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
)

func TestActionStatusEntrypointNameAndLegacyEvents(t *testing.T) {
	for _, name := range []string{"train", "evaluate", ""} {
		value, err := cbor.Marshal(actionStatusEvent{
			Type: "task.status.v2", ActionID: uuid.NewString(), EntrypointID: 1,
			EntrypointName: name, Status: "running", UpdatedAt: "2026-10-06T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
		event, err := (Service{}).DecodeActionStatus(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if event.EntrypointName != name || event.EntrypointId != 1 {
			t.Fatalf("decoded entrypoint: %+v", event)
		}
	}
}
