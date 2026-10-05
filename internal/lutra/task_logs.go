package lutra

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
)

const maxLogLine = 64 << 10

type taskLogSink struct {
	writers map[LogPhase]*logLineWriter
	events  chan runlog.TaskLogEvent
	done    chan struct{}
	close   sync.Once
}

type logLineWriter struct {
	mu       sync.Mutex
	sink     *taskLogSink
	metadata runlog.TaskLogEvent
	buffer   []byte
}

func (w *SandboxRuntime) newLogSink(task *EnvironmentExecution, runtime, image string) *taskLogSink {
	runID := uuid.MustParse(task.RunID)
	return newTaskLogSink(runlog.TaskLogEvent{ActionID: task.ActionID, Attempt: max(task.Attempt, 1), Runtime: runtime, Image: image}, func(ctx context.Context, appendID string, entries []*lutrav1.LogEntry) error {
		_, err := w.Logs.AppendEntries(ctx, runID, runlog.TaskLogStream, appendID, entries)
		return err
	})
}

func newTaskLogSink(metadata runlog.TaskLogEvent, appendEntries func(context.Context, string, []*lutrav1.LogEntry) error) *taskLogSink {
	s := &taskLogSink{writers: make(map[LogPhase]*logLineWriter), events: make(chan runlog.TaskLogEvent, 32), done: make(chan struct{})}
	for _, phase := range []LogPhase{LogTask, LogBuild, LogPull} {
		event := metadata
		event.Phase = string(phase)
		if phase == LogTask {
			event.Runtime, event.Image = "", ""
		}
		s.writers[phase] = &logLineWriter{sink: s, metadata: event}
	}
	go s.collect(metadata, appendEntries)
	return s
}

func (s *taskLogSink) Writer(phase LogPhase) io.Writer { return s.writers[phase] }

func (w *logLineWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	length := len(data)
	for len(data) > 0 {
		size := min(len(data), maxLogLine-len(w.buffer))
		if newline := bytes.IndexByte(data[:size], '\n'); newline >= 0 {
			w.buffer = append(w.buffer, data[:newline]...)
			w.emit()
			data = data[newline+1:]
			continue
		}
		w.buffer = append(w.buffer, data[:size]...)
		data = data[size:]
		if len(w.buffer) == maxLogLine {
			start := len(w.buffer) - 1
			for start > 0 && !utf8.RuneStart(w.buffer[start]) {
				start--
			}
			if utf8.FullRune(w.buffer[start:]) {
				w.emit()
			} else {
				tail := bytes.Clone(w.buffer[start:])
				w.buffer = w.buffer[:start]
				w.emit()
				w.buffer = append(w.buffer, tail...)
			}
		}
	}
	return length, nil
}

func (w *logLineWriter) emit() {
	event := w.metadata
	event.Type, event.Source = "task.log.v1", "stderr"
	event.Message = strings.ToValidUTF8(strings.TrimSuffix(string(w.buffer), "\r"), "?")
	event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	w.sink.events <- event
	w.buffer = w.buffer[:0]
}

// Producers must stop before Close; Close drains the final partial lines and batch.
func (s *taskLogSink) Close() {
	s.close.Do(func() {
		for _, phase := range []LogPhase{LogTask, LogBuild, LogPull} {
			writer := s.writers[phase]
			if len(writer.buffer) > 0 {
				writer.emit()
			}
		}
		close(s.events)
		<-s.done
	})
}

func (s *taskLogSink) collect(metadata runlog.TaskLogEvent, appendEntries func(context.Context, string, []*lutrav1.LogEntry) error) {
	defer close(s.done)
	collectionID := uuid.NewString()
	batch := make([]*lutrav1.LogEntry, 0, 32)
	batchNumber := 0
	flush := func() {
		if len(batch) == 0 {
			return
		}
		batchNumber++
		appendID := fmt.Sprintf("task-log:%s:%s:%d", metadata.ActionID, collectionID, batchNumber)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := appendEntries(ctx, appendID, batch); err != nil {
			slog.Error("append task log", "action_id", metadata.ActionID, "attempt", metadata.Attempt, "error", err)
		}
		cancel()
		batch = batch[:0]
	}
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				flush()
				return
			}
			value, err := runlog.EncodeTaskLog(event)
			if err != nil {
				slog.Error("encode task log", "action_id", metadata.ActionID, "error", err)
				continue
			}
			batch = append(batch, &lutrav1.LogEntry{Key: []byte(metadata.ActionID), ValueCbor: value})
			if len(batch) == cap(batch) {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}
