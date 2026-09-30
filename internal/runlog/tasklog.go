package runlog

import (
	"bytes"
	"errors"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
)

const TaskLogStream = "task_log"

// TaskLogEvent is the versioned payload stored in TaskLogStream.
type TaskLogEvent struct {
	Type      string `cbor:"type"`
	Source    string `cbor:"source"`
	Message   string `cbor:"message"`
	Timestamp string `cbor:"timestamp"`
	ActionID  string `cbor:"action_id"`
	Attempt   int32  `cbor:"attempt"`
}

func EncodeTaskLog(event TaskLogEvent) ([]byte, error) {
	if err := validateTaskLog(event); err != nil {
		return nil, err
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal(event)
}

func DecodeTaskLog(value []byte) (TaskLogEvent, error) {
	var event TaskLogEvent
	if err := cbor.Unmarshal(value, &event); err != nil {
		return event, err
	}
	if err := validateTaskLog(event); err != nil {
		return event, err
	}
	canonical, err := EncodeTaskLog(event)
	if err != nil {
		return event, err
	}
	if !bytes.Equal(canonical, value) {
		return event, errors.New("task log event is not canonical CBOR")
	}
	return event, nil
}

func validateTaskLog(event TaskLogEvent) error {
	if event.Type != "task.log.v1" || event.Source != "stderr" {
		return errors.New("invalid task log event")
	}
	if _, err := uuid.Parse(event.ActionID); err != nil {
		return errors.New("invalid task log action id")
	}
	if event.Attempt <= 0 {
		return errors.New("invalid task log attempt")
	}
	if _, err := time.Parse(time.RFC3339Nano, event.Timestamp); err != nil {
		return err
	}
	return nil
}
