package lutra

import (
	"context"
	"os"
	"testing"

	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunLeaseCannotMutateAfterTransactionDeadline(t *testing.T) {
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LUTRA_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id, token, environment, action := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, "INSERT INTO lutra.runs (id, namespace_id, claim_token, lease_until) SELECT $1, id, $2, transaction_timestamp() + interval '20 milliseconds' FROM lutra.namespaces LIMIT 1", id, token); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO lutra.task_environments (id, namespace_id, name, version, provider, spec, image_key) SELECT $1, namespace_id, $2, 'test', 'test', '\\xf6'::bytea, decode(repeat('00', 32), 'hex') FROM lutra.runs WHERE id = $3", environment, environment.String(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO lutra.task_actions (id, run_id, environment_id, entrypoint_id, action_spec, status) VALUES ($1, $2, $3, 1, '\\xf6'::bytea, 'running')", action, id, environment); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_sleep(0.05)"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.New(tx).RenewRunLease(ctx, db.RenewRunLeaseParams{RunID: id, ClaimToken: token, LeaseSeconds: 30})
	if err != nil || rows != 0 {
		t.Fatalf("expired lease renewed in old transaction: rows=%d, err=%v", rows, err)
	}
	rows, err = db.New(tx).TransitionRunTaskAction(ctx, db.TransitionRunTaskActionParams{
		RunID: id, ClaimToken: token, ActionID: action, Status: db.LutraTaskActionStatusFailed,
	})
	if err != nil || rows != 0 {
		t.Fatalf("expired lease transitioned action in old transaction: rows=%d, err=%v", rows, err)
	}
	rows, err = db.New(tx).ResetRunActions(ctx, db.ResetRunActionsParams{RunID: id, ClaimToken: token})
	if err != nil || rows != 0 {
		t.Fatalf("expired lease reset action in old transaction: rows=%d, err=%v", rows, err)
	}
}
