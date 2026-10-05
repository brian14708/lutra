package lutra

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/runlog"
)

func TestTaskLogSinkFramesPhasesAndDrains(t *testing.T) {
	var events []runlog.TaskLogEvent
	sink := newTaskLogSink(runlog.TaskLogEvent{ActionID: "00000000-0000-0000-0000-000000000001", Attempt: 2, Runtime: "podman", Image: "image"}, func(_ context.Context, id string, entries []*lutrav1.LogEntry) error {
		if id == "" {
			t.Error("missing append identity")
		}
		for _, entry := range entries {
			event, err := runlog.DecodeTaskLog(entry.ValueCbor)
			if err != nil {
				t.Error(err)
				continue
			}
			events = append(events, event)
		}
		return nil
	})
	_, _ = io.WriteString(sink.Writer(LogPull), "pull\r\n")
	_, _ = io.WriteString(sink.Writer(LogBuild), "bu")
	_, _ = io.WriteString(sink.Writer(LogBuild), "ild\n")
	_, _ = io.WriteString(sink.Writer(LogTask), "task\npartial")
	sink.Close()
	sink.Close()
	if len(events) != 4 {
		t.Fatalf("events = %v", events)
	}
	for index, expected := range []struct{ phase, message string }{{"pull", "pull"}, {"build", "build"}, {"task", "task"}, {"task", "partial"}} {
		event := events[index]
		if event.Phase != expected.phase || event.Message != expected.message || event.Attempt != 2 {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
	if events[2].Runtime != "" || events[2].Image != "" {
		t.Fatal("task output retained build metadata")
	}
}

func TestTaskLogSinkBoundsLinesAndSurvivesAppendFailure(t *testing.T) {
	var messages []string
	sink := newTaskLogSink(runlog.TaskLogEvent{ActionID: "00000000-0000-0000-0000-000000000001", Attempt: 1}, func(_ context.Context, _ string, entries []*lutrav1.LogEntry) error {
		for _, entry := range entries {
			event, err := runlog.DecodeTaskLog(entry.ValueCbor)
			if err != nil {
				t.Error(err)
			}
			messages = append(messages, event.Message)
		}
		return errors.New("offline")
	})
	data := bytes.Repeat([]byte("x"), maxLogLine+3)
	if n, err := sink.Writer(LogTask).Write(data); n != len(data) || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	sink.Close()
	if len(messages) != 2 || len(messages[0]) != maxLogLine || messages[1] != "xxx" {
		t.Fatalf("messages = %d", len(messages))
	}
}

func TestSandboxCapacityCancellationReleasesQueue(t *testing.T) {
	t.Setenv("LUTRA_WORKER_CONCURRENCY", "1")
	runtime, err := newSandboxRuntime(nil, runlog.Service{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	release, err := runtime.acquire(t.Context(), LogBuild)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := runtime.acquire(ctx, LogTask); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued acquisition = %v", err)
	}
	release()
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	release, err = runtime.acquire(ctx, LogTask)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestTaskLogSinkKeepsUTF8AcrossLineLimit(t *testing.T) {
	var messages []string
	sink := newTaskLogSink(runlog.TaskLogEvent{ActionID: "00000000-0000-0000-0000-000000000001", Attempt: 1}, func(_ context.Context, _ string, entries []*lutrav1.LogEntry) error {
		for _, entry := range entries {
			event, err := runlog.DecodeTaskLog(entry.ValueCbor)
			if err != nil {
				t.Error(err)
			}
			messages = append(messages, event.Message)
		}
		return nil
	})
	line := strings.Repeat("x", maxLogLine-1) + "\u20ac"
	_, _ = io.WriteString(sink.Writer(LogTask), line)
	sink.Close()
	if strings.Join(messages, "") != line {
		t.Fatal("UTF-8 was corrupted at line boundary")
	}
}
