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
	"google.golang.org/protobuf/proto"
)

type actionStatusEvent struct {
	Type           string `cbor:"type"`
	ActionID       string `cbor:"action_id"`
	CallerActionID string `cbor:"caller_action_id"`
	EntrypointID   uint32 `cbor:"entrypoint_id"`
	Status         string `cbor:"status"`
	Attempt        int32  `cbor:"attempt"`
	ResultCBOR     []byte `cbor:"result_cbor"`
	UpdatedAt      string `cbor:"updated_at"`
	MaxAttempts    int32  `cbor:"max_attempts"`
	CacheHit       bool   `cbor:"cache_hit"`
}

func (s Service) DecodeActionStatus(ctx context.Context, value []byte) (*lutrav1.TaskActionStatus, error) {
	var event actionStatusEvent
	value, err := s.resolveValue(ctx, value)
	if err != nil {
		return nil, err
	}
	if err := cbor.Unmarshal(value, &event); err != nil {
		return nil, err
	}
	if event.Type != "task.status.v2" {
		return nil, errors.New("unknown task status event type or version")
	}
	if _, err := uuid.Parse(event.ActionID); err != nil {
		return nil, err
	}
	if event.CallerActionID != "" {
		if _, err := uuid.Parse(event.CallerActionID); err != nil {
			return nil, err
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, event.UpdatedAt); err != nil {
		return nil, err
	}
	switch event.Status {
	case "queued", "building", "running", "waiting", "succeeded", "failed", "canceled":
	default:
		return nil, errors.New("invalid task status event")
	}
	return &lutrav1.TaskActionStatus{ActionId: event.ActionID, CallerActionId: event.CallerActionID, EntrypointId: event.EntrypointID, Status: event.Status, Attempt: event.Attempt, ResultCbor: event.ResultCBOR, UpdatedAt: event.UpdatedAt, MaxAttempts: event.MaxAttempts, CacheHit: event.CacheHit}, nil
}

func (s Service) AppendActionStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID, cacheHit bool) error {
	row, err := db.New(tx).ReadTaskAction(ctx, id)
	if err != nil {
		return err
	}
	if row.CallerActionID == nil && !cacheHit {
		return nil
	}
	caller := ""
	if row.CallerActionID != nil {
		caller = row.CallerActionID.String()
	}
	var spec lutrav1.ActionSpec
	if err := proto.Unmarshal(row.ActionSpec, &spec); err != nil {
		return err
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	value, err := mode.Marshal(actionStatusEvent{Type: "task.status.v2", ActionID: id.String(), CallerActionID: caller, EntrypointID: uint32(row.EntrypointID), Status: string(row.Status), Attempt: row.Attempts, ResultCBOR: row.ResultCbor, UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano), MaxAttempts: spec.MaxAttempts, CacheHit: cacheHit})
	if err != nil {
		return err
	}
	_, err = s.AppendTx(ctx, tx, row.RunID, StatusStream, "", []*lutrav1.LogEntry{{Key: []byte(id.String()), ValueCbor: value}})
	return err
}
