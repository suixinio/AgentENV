package routing

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestReaderGetHit reads back, through a real Redis, the exact bytes the live
// writer puts there.
//
// 🔴 The value written is a literal — see record_test.go's
// storedRecordWithPodName / storedRecordWithoutPodName and the note above them.
// It used to be produced by this package's own MarshalRecord, which made this
// the one test in the module that touches real Redis and still proved nothing
// about the format: Go encoded it, Go decoded it, and any shape at all would
// have passed while the process that actually writes these keys is `aenv-api`.
func TestReaderGetHit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		stored      string
		wantPodName string
	}{
		{name: "with a pod name", stored: storedRecordWithPodName, wantPodName: "agentenv-node-7f4c2"},
		{name: "without one", stored: storedRecordWithoutPodName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, client := newReaderForTest(t)
			writeKey(t, client, BindingKey(DefaultKeyPrefix, "sbx-1"), tc.stored)

			got, ok, err := reader.Get(context.Background(), " sbx-1 ")
			if err != nil || !ok {
				t.Fatalf("expected a hit, got (%+v, %v, %v)", got, ok, err)
			}
			if got.Node.ID != "node-a" || got.Node.Endpoint != "http://node-a" {
				t.Fatalf("unexpected node: %+v", got.Node)
			}
			if got.Node.PodName != tc.wantPodName {
				t.Fatalf("pod name = %q, want %q", got.Node.PodName, tc.wantPodName)
			}
			if got.ExecutionID != storedRecordExecutionID {
				t.Fatalf("execution id = %q, want %q", got.ExecutionID, storedRecordExecutionID)
			}
		})
	}
}

// TestReaderGetMissIsNotAnError is the property the whole read path rests on:
// a miss and a failure are different things, and neither of them is an absence.
func TestReaderGetMissIsNotAnError(t *testing.T) {
	reader, client := newReaderForTest(t)

	got, ok, err := reader.Get(context.Background(), "never-existed")
	if err != nil || ok || got != (Record{}) {
		t.Fatalf("a missing key must be a clean miss, got (%+v, %v, %v)", got, ok, err)
	}

	// An undecodable record is also a miss rather than an error: it names
	// nowhere to forward to, and the caller's fallback will produce a real
	// answer.
	writeKey(t, client, BindingKey(DefaultKeyPrefix, "garbage"), "not-json")
	got, ok, err = reader.Get(context.Background(), "garbage")
	if err != nil || ok || got != (Record{}) {
		t.Fatalf("an undecodable record must be a clean miss, got (%+v, %v, %v)", got, ok, err)
	}

	// A record naming no endpoint is the same: it decodes, and it routes
	// nowhere.
	writeKey(t, client, BindingKey(DefaultKeyPrefix, "no-endpoint"), `{"node":{"node_id":"node-a","endpoint":""}}`)
	if _, ok, err := reader.Get(context.Background(), "no-endpoint"); err != nil || ok {
		t.Fatalf("a record with no endpoint must be a clean miss, got (%v, %v)", ok, err)
	}

	// A reservation decodes and names a real node, and is still a miss: the
	// create that put it there has not finished, so the api half is the only
	// thing that may answer for this sandbox yet.
	writeKey(t, client, BindingKey(DefaultKeyPrefix, "reserved"), storedReservationRecord)
	if got, ok, err := reader.Get(context.Background(), "reserved"); err != nil || ok || got != (Record{}) {
		t.Fatalf("a reservation must be a clean miss, got (%+v, %v, %v)", got, ok, err)
	}
	// The same key once the node has acknowledged the sandbox.
	writeKey(t, client, BindingKey(DefaultKeyPrefix, "reserved"), storedReservationConfirmed)
	if got, ok, err := reader.Get(context.Background(), "reserved"); err != nil || !ok || got.Node.ID != "node-a" {
		t.Fatalf("the confirmation must be a hit, got (%+v, %v, %v)", got, ok, err)
	}

	if _, ok, err := reader.Get(context.Background(), "   "); err != nil || ok {
		t.Fatalf("a blank sandbox id must be a clean miss, got (%v, %v)", ok, err)
	}
}

// TestReaderGetFailureIsAnError separates the third outcome from the second.
// The caller must be able to tell "there is no record" from "I could not look",
// because only one of those may ever contribute to a 404.
func TestReaderGetFailureIsAnError(t *testing.T) {
	reader, client := newReaderForTest(t)
	if err := client.Close(); err != nil {
		t.Fatalf("close redis client failed: %v", err)
	}
	_ = reader.Close()

	_, ok, err := reader.Get(context.Background(), "sbx-1")
	if err == nil {
		t.Fatal("a read against a closed client must report an error, not a miss")
	}
	if ok {
		t.Fatal("a failed read must never report a hit")
	}
}

// TestReaderGetHonoursCallerCancellation keeps the read inside the request's
// own deadline rather than only inside the reader's.
func TestReaderGetHonoursCallerCancellation(t *testing.T) {
	reader, _ := newReaderForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, err := reader.Get(ctx, "sbx-1")
	if ok {
		t.Fatal("a cancelled read must never report a hit")
	}
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the caller's cancellation to surface, got %v", err)
	}
}

func TestNewReaderRejectsBlankAddress(t *testing.T) {
	if _, err := NewReader("   "); err == nil {
		t.Fatal("a blank address must fail at construction rather than per request")
	}
}

func newReaderForTest(t *testing.T) (*Reader, *redis.Client) {
	t.Helper()
	addr := startRedisServerForTest(t)
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	reader, err := NewReader(addr)
	if err != nil {
		t.Fatalf("create routing reader failed: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader, client
}

func writeKey(t *testing.T, client *redis.Client, key string, value string) {
	t.Helper()
	if err := client.Set(context.Background(), key, value, time.Minute).Err(); err != nil {
		t.Fatalf("write %s failed: %v", key, err)
	}
}

// startRedisServerForTest mirrors the scheduler package's bootstrap, including
// its escape hatch.
//
// 🔴 The duplication is deliberate: Go's internal rule keeps this package out of
// the scheduler's test helpers, and the alternative — leaving the reader
// untested because the harness lives elsewhere — is how the one component that
// only runs in the Redis deployment ends up with no coverage at all.
func startRedisServerForTest(t *testing.T) string {
	t.Helper()

	bin := strings.TrimSpace(os.Getenv("REDIS_SERVER_BIN"))
	if bin == "" {
		var err error
		bin, err = exec.LookPath("redis-server")
		if err != nil {
			// 🔴 AENV_REDIS_TEST_REQUIRED, not SCHEDULER_REDIS_TEST_REQUIRED:
			// the same name the Rust harness reads, deliberately. The two
			// languages read one Redis key format, and a run that set one
			// variable and not the other would arm half the contract while
			// reporting green for all of it.
			if os.Getenv("AENV_REDIS_TEST_REQUIRED") != "" {
				t.Fatal("AENV_REDIS_TEST_REQUIRED is set but no redis-server was found: these routing-reader tests would have been skipped")
			}
			t.Skip("redis-server not found; set REDIS_SERVER_BIN or install redis-server to run the routing reader test")
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate redis test port failed: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("parse redis test listener addr failed: %v", err)
	}
	_ = listener.Close()

	var output bytes.Buffer
	cmd := exec.Command(
		bin,
		"--bind", "127.0.0.1",
		"--port", port,
		"--save", "",
		"--appendonly", "no",
		"--dir", t.TempDir(),
		"--loglevel", "warning",
	)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server failed: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	addr := net.JoinHostPort("127.0.0.1", port)
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return addr
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("redis-server exited before readiness: %s", output.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("redis-server did not become ready; output: %s", output.String())
	return ""
}
