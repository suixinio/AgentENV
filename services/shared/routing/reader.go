package routing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultOperationTimeout bounds one projection read. It matches the timeout
// the scheduler's own binding store uses, because it is the same store.
const DefaultOperationTimeout = 2 * time.Second

// Reader is the read half of the projection, for a process that owns none of
// the writes.
//
// 🔴 It reads and nothing else. Everything about the record's lifetime — when
// it is installed, refreshed, superseded, deleted — belongs to the scheduler,
// and a reader that repaired what it found would be a second writer with none
// of the arbitration.
type Reader struct {
	client    *redis.Client
	keyPrefix string
	timeout   time.Duration
}

// NewReader dials Redis and proves the connection before returning, so a
// misconfigured address stops the process at start-up rather than becoming a
// per-request warning.
func NewReader(addr string) (*Reader, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	var opts *redis.Options
	if strings.Contains(addr, "://") {
		parsed, err := redis.ParseURL(addr)
		if err != nil {
			return nil, fmt.Errorf("parse redis address: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: addr}
	}

	reader := &Reader{
		client:    redis.NewClient(opts),
		keyPrefix: DefaultKeyPrefix,
		timeout:   DefaultOperationTimeout,
	}
	ctx, cancel := context.WithTimeout(context.Background(), reader.timeout)
	defer cancel()
	if err := reader.client.Ping(ctx).Err(); err != nil {
		_ = reader.client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return reader, nil
}

// NewReaderWithClient wraps an already-built client. Tests use it; so would a
// process that shares one connection pool across several readers.
func NewReaderWithClient(client *redis.Client) *Reader {
	return &Reader{client: client, keyPrefix: DefaultKeyPrefix, timeout: DefaultOperationTimeout}
}

func (r *Reader) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

// Get returns the record for a sandbox.
//
// The three outcomes are kept apart on purpose and the caller must keep them
// apart too:
//
//   - (record, true, nil):  a hit, and the answer.
//   - (zero, false, nil):   a miss. 🔴 Not an absence — the projection is one
//     of several things that know where a sandbox is, and the others have to be
//     asked before anybody says "nowhere".
//   - (zero, false, err):   could not look. Also not an absence, and in
//     particular not a reason to fail the request: the caller falls back to the
//     scheduler, which is what it did before this reader existed.
//
// An undecodable record is a miss. It names nowhere to forward to, and the
// fallback will produce a real answer. So is a record whose create has not
// finished: see ParseRecord.
func (r *Reader) Get(ctx context.Context, sandboxID string) (Record, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return Record{}, false, nil
	}

	opCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	raw, err := r.client.Get(opCtx, BindingKey(r.keyPrefix, sandboxID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("redis get routing projection: %w", err)
	}
	record, ok := ParseRecord(raw)
	if !ok {
		return Record{}, false, nil
	}
	return record, true, nil
}
