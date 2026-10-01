package lutra

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCommittedCancellationNotificationReachesWorker(t *testing.T) {
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LUTRA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if os.Getenv("LUTRA_TEST_NOTIFICATION_CHILD") == "1" {
		id, err := uuid.Parse(os.Getenv("LUTRA_TEST_NOTIFICATION_ID"))
		if err != nil {
			t.Fatal(err)
		}
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		owner, stopOwner := context.WithCancel(context.Background())
		defer stopOwner()
		worker := &RunWorker{DB: pool, owners: map[uuid.UUID]context.CancelFunc{id: stopOwner}}
		ready := make(chan struct{})
		listenCtx, stopListening := context.WithCancel(ctx)
		finished := make(chan struct{})
		go func() { worker.listenCancels(listenCtx, ready); close(finished) }()
		defer func() { stopListening(); <-finished }()
		select {
		case <-ready:
			if _, err := fmt.Fprintln(os.Stdout, "READY", id); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("worker did not start listening")
		}
		select {
		case <-owner.Done():
			return
		case <-ctx.Done():
			t.Fatal("worker did not receive committed cancellation")
		}
	}
	sender, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	id := uuid.New()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommittedCancellationNotificationReachesWorker$")
	command.Env = append(os.Environ(), "LUTRA_TEST_NOTIFICATION_CHILD=1", "LUTRA_TEST_NOTIFICATION_ID="+id.String())
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || ready != "READY "+id.String()+"\n" {
		_ = command.Wait()
		t.Fatalf("worker did not listen: %q, %v, %s", ready, err, stderr.String())
	}
	tx, err := sender.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "SELECT pg_notify('lutra_run_cancel', $1)", id.String()); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = command.Wait(); err != nil {
		t.Fatalf("worker process failed: %v, %s", err, stderr.String())
	}
}
