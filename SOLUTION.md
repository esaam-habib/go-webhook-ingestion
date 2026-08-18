# SOLUTION.md

## What was broken, and why

**1. Data race in the stats cache (`stats.Cache.Record`)** 

`Record` read and wrote the shared map without holding any lock, while `Get`
held a read lock. Concurrent webhook deliveries would corrupt the in-memory
counters and could panic on a concurrent map write. Fixed by acquiring the
write lock in `Record`, verified with a `-race` test that hammers `Record`
from 50 goroutines.

**2. Background goroutine inherited a cancelled context** 

`processRecording` was launched with the HTTP request's context. Go cancels
that context as soon as the handler writes its response, which is before the
goroutine's 50 ms sleep even finishes. The `UPDATE` that sets
`recording_processed = TRUE` always ran against an already-cancelled context,
so it always failed. The error was silently swallowed (`// TODO: handle`), so
nothing appeared in the logs — this is the "recordings never get marked
processed" and "nothing in the logs about it" symptom from the ops report.
Fixed by deriving a detached context with `context.WithoutCancel` before
spawning the goroutine, and logging the error instead of dropping it.

**3. Non-atomic deduplication (TOCTOU race)** 

The original guard was `EventExists` (SELECT) → `InsertEvent` (INSERT). Two
concurrent or retried deliveries of the same `event_id` could both pass the
existence check before either had inserted, then both proceed to insert,
increment `account_stats`, and update the in-memory cache — doubling every
counter. The `events` table had only a plain index on `event_id`, not a
UNIQUE constraint, so the database did nothing to prevent this. This was the
"duplicate call records" / "call-counts drifting higher" symptom. Fixed by
adding a UNIQUE constraint (migration `002_events_event_id_unique.sql`) and
rewriting `InsertEvent` to use `INSERT ... ON CONFLICT (event_id) DO NOTHING`,
returning whether the row was actually inserted. `Ingest` gates all
downstream writes (`UpsertCall`, `IncrementAccountStats`, cache update, and
recording processing) on that boolean, making deduplication a single atomic
database operation instead of a check-then-act race.

**4. In-memory cache lost on every restart** 

`stats.Cache` had a `Seed` method for warming it from durable storage, but
nothing ever called it. `stats.NewCache()` always starts empty, and `GET
/accounts/{id}/stats` reads only from the cache, never from Postgres. So
every restart or deploy reset visible stats to zero for every account, even
though `account_stats` in Postgres was untouched — part of the "every time
we deploy, whatever was in flight seems to just disappear" symptom. Fixed by
adding `Store.AllAccountStats` (reads every row from `account_stats`) and
calling it once in `main()` at startup to seed the cache via the existing
`Seed` method, before the HTTP server starts accepting traffic.

**5. In-flight background work killed on shutdown** 

This is a second, distinct cause of the same "whatever was in flight just
disappears" symptom. `Ingest` returns to the HTTP handler as soon as it
spawns the recording-processing goroutine (fix #2 made that goroutine
correct, but the handler still doesn't wait for it). `srv.Shutdown()` only
waits for active HTTP handlers to finish — it has no visibility into detached
goroutines — so on `SIGTERM` the process could exit while a
`processRecording` goroutine was still mid-flight, silently losing it with no
log line at all. Fixed by tracking background goroutines in `Service` with a
`sync.WaitGroup` and adding `Service.Shutdown(ctx)`, which `main()` now calls
right after `srv.Shutdown`, so the process waits (bounded by the same
shutdown deadline) for in-flight recording processing to finish before
exiting.

---

## Why this deduplication strategy

The alternatives I considered:

- **Redis SET NX (distributed lock or seen-set).** Would work, but adds a
  second round-trip and a second system to keep consistent with Postgres. If
  Redis is unavailable or the key expires, the guarantee evaporates. For an
  at-least-once webhook stream where `event_id` is the natural idempotency key,
  the canonical record of "have we seen this?" belongs in the same durable
  store as the event itself.

- **Application-level lock (mutex / singleflight).** Only protects within a
  single process. A second pod or a restart defeats it entirely.

- **UNIQUE constraint + `ON CONFLICT DO NOTHING`.** The database enforces
  uniqueness atomically under any isolation level. There is no window between
  the check and the insert. No second system to synchronize. No expiry to
  reason about. The cost is one extra index write per insert, which is
  negligible compared to the rest of the write path. This is the simplest
  approach that is correct under concurrency and across restarts, which is
  why it's what I shipped.

---

## What I would change at 10,000 webhooks/second

At that scale the single-writer Postgres path becomes the bottleneck before
anything else does. The changes I would make, roughly in order of impact:

1. **Decouple ingestion from processing.** The handler should only write the
   raw event to a durable queue (Kafka, SQS, or Postgres itself via
   `LISTEN/NOTIFY`) and return 200 immediately. Workers consume from the queue,
   handle deduplication, and update stats. This absorbs traffic spikes without
   back-pressure reaching the provider.

2. **Partition `account_stats` updates.** At high fan-in, many workers
   contending on the same `account_id` row serializes on a row lock. Options:
   write deltas to an append-only table and aggregate on read, or use a
   streaming aggregator (Flink, Redis sorted sets) for the hot counters and
   sync to Postgres periodically.

3. **Replace the cold-start warmup with a read-through cache.** Loading all
   of `account_stats` at startup becomes slow when there are millions of
   accounts. Instead, populate the cache lazily on first read and accept a
   single DB hit per account after a restart.

4. **Idempotency key in the queue layer.** With a queue in front, use the
   queue's own deduplication window (Kafka compaction, SQS message
   deduplication ID) as a first cheap filter, keeping the UNIQUE constraint
   as a backstop for redeliveries that slip through.

5. **Bound background work instead of an unbounded goroutine-per-event.** The
   current `Service.Shutdown` waits for all in-flight recording processing to
   drain, but there's no cap on how many goroutines can be in flight at once.
   At 10k/s, that needs a worker pool (or the queue from #1) with backpressure,
   so shutdown has a bounded amount of work to drain rather than an
   unbounded one.
