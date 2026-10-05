package lutra

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type workflowEvent struct {
	Sequence    uint64
	Fingerprint [32]byte
	Finished    bool
}

// Callback channels are local capabilities; only command identity enters the journal.
type workflowInbox struct {
	ctx       context.Context
	dispatch  *Dispatcher
	mu        sync.Mutex
	calls     map[uint64]workflowCallback
	changed   chan struct{}
	sequence  uint64
	awakeable string
	published bool
	finished  bool
	consumed  uint64
}

func newWorkflowInbox(ctx context.Context, dispatch *Dispatcher) *workflowInbox {
	return &workflowInbox{ctx: ctx, dispatch: dispatch, calls: make(map[uint64]workflowCallback), changed: make(chan struct{})}
}

func (q *workflowInbox) submit(ctx context.Context, call workflowCallback) error {
	field := call.request.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("sequence"))
	sequence := call.request.ProtoReflect().Get(field).Uint()
	if sequence == 0 {
		return invalid("workflow commands require a positive sequence")
	}
	q.mu.Lock()
	if _, exists := q.calls[sequence]; exists || sequence <= q.consumed {
		q.mu.Unlock()
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("duplicate workflow command sequence"))
	}
	q.calls[sequence] = call
	close(q.changed)
	q.changed = make(chan struct{})
	q.publishLocked()
	q.mu.Unlock()
	return nil
}

func (q *workflowInbox) register(sequence uint64, id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.sequence, q.awakeable, q.published = sequence, id, false
	q.publishLocked()
}

func (q *workflowInbox) finish() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.finished = true
	close(q.changed)
	q.changed = make(chan struct{})
	q.publishLocked()
}

func (q *workflowInbox) publishLocked() {
	if q.awakeable == "" || q.published {
		return
	}
	call, present := q.calls[q.sequence]
	if !present && !q.finished {
		return
	}
	event := workflowEvent{Sequence: q.sequence, Finished: !present}
	if present {
		event.Fingerprint, _ = commandFingerprint(call.request)
	}
	q.published = true
	id := q.awakeable
	go func() {
		for {
			ctx, cancel := context.WithTimeout(q.ctx, 15*time.Second)
			_, err := q.dispatch.request(ctx, http.MethodPost, q.dispatch.Ingress+"/restate/awakeables/"+id+"/resolve", event, "")
			cancel()
			if err == nil || q.ctx.Err() != nil {
				return
			}
			select {
			case <-time.After(100 * time.Millisecond):
			case <-q.ctx.Done():
				return
			}
		}
	}()
}

func (q *workflowInbox) take(ctx context.Context, event workflowEvent) (workflowCallback, error) {
	for {
		q.mu.Lock()
		call, present := q.calls[event.Sequence]
		finished, changed := q.finished, q.changed
		if present {
			delete(q.calls, event.Sequence)
			q.consumed = event.Sequence
		}
		q.mu.Unlock()
		if present {
			fingerprint, err := commandFingerprint(call.request)
			if err != nil {
				return workflowCallback{}, err
			}
			if event.Finished || fingerprint != event.Fingerprint {
				return workflowCallback{}, errors.New("workflow command changed during replay")
			}
			return call, nil
		}
		if finished {
			if !event.Finished {
				return workflowCallback{}, errors.New("workflow finished before its recorded command")
			}
			return workflowCallback{}, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return workflowCallback{}, ctx.Err()
		}
	}
}
