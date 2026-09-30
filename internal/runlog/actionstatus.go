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

type actionStatusEvent struct {
	Type           string `cbor:"type"`
	ActionID       string `cbor:"action_id"`
	CallerActionID string `cbor:"caller_action_id"`
	EntrypointID   uint32 `cbor:"entrypoint_id"`
	Status         string `cbor:"status"`
	Attempt        int32  `cbor:"attempt"`
	Error          string `cbor:"error"`
	UpdatedAt      string `cbor:"updated_at"`
}

func DecodeActionStatus(value []byte) (*lutrav1.TaskActionStatus, error) {
	var event actionStatusEvent
	if err := cbor.Unmarshal(value, &event); err != nil {
		return nil, err
	}
	if event.Type != "task.status.v1" {
		return nil, errors.New("unknown task status event type or version")
	}
	if _, err := uuid.Parse(event.ActionID); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(event.CallerActionID); err != nil {
		return nil, err
	}
	if _, err := time.Parse(time.RFC3339Nano, event.UpdatedAt); err != nil {
		return nil, err
	}
	switch event.Status {
	case "queued", "running", "waiting", "succeeded", "failed", "canceled":
	default:
		return nil, errors.New("invalid task status event")
	}
	return &lutrav1.TaskActionStatus{ActionId: event.ActionID, CallerActionId: event.CallerActionID, EntrypointId: event.EntrypointID, Status: event.Status, Attempt: event.Attempt, Error: event.Error, UpdatedAt: event.UpdatedAt}, nil
}

func (s Service) AppendActionStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	row, err := db.New(tx).ReadTaskAction(ctx, id)
	if err != nil {
		return err
	}
	if row.CallerActionID == nil {
		return nil
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	value, err := mode.Marshal(actionStatusEvent{Type: "task.status.v1", ActionID: id.String(), CallerActionID: row.CallerActionID.String(), EntrypointID: uint32(row.EntrypointID), Status: string(row.Status), Attempt: row.Attempts, Error: row.Error, UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	_, err = s.AppendTx(ctx, tx, row.RunID, StatusStream, "", []*lutrav1.LogEntry{{Key: []byte(id.String()), ValueCbor: value}})
	return err
}
