package runlog

import (
	"context"
	"errors"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const StatusStream = "__run_status"

// StatusEvent contains task output as opaque bytes inside a versioned CBOR envelope.
type StatusEvent struct {
	Type       string `cbor:"type"`
	Status     string `cbor:"status"`
	OutputCBOR []byte `cbor:"output_cbor"`
	Error      string `cbor:"error"`
	UpdatedAt  string `cbor:"updated_at"`
}

func (s Service) DecodeStatus(ctx context.Context, value []byte) (StatusEvent, error) {
	var event StatusEvent
	value, err := s.resolveValue(ctx, value)
	if err != nil {
		return event, err
	}
	if err := cbor.Unmarshal(value, &event); err != nil {
		return event, err
	}
	if event.Type != "run.status.v1" {
		return event, errors.New("unknown run status event type or version")
	}
	switch event.Status {
	case "queued", "building", "running", "waiting", "succeeded", "failed", "canceled":
	default:
		return event, errors.New("invalid run status event")
	}
	if _, err := time.Parse(time.RFC3339Nano, event.UpdatedAt); err != nil {
		return event, err
	}
	return event, nil
}

// AppendStatus records the current row in the transaction that changed it.
func (s Service) AppendStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	row, err := db.New(tx).ReadRun(ctx, id)
	if err != nil {
		return err
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	output := row.OutputCbor
	if output == nil {
		output = []byte{}
	}
	value, err := mode.Marshal(StatusEvent{
		Type: "run.status.v1", Status: string(row.Status),
		OutputCBOR: output, Error: row.Error, UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	_, err = s.AppendTx(ctx, tx, id, StatusStream, "", []*lutrav1.LogEntry{{Key: []byte("status"), ValueCbor: value}})
	return err
}
