# Ping-Pong Multi-Process Simulation

A simulation of one or more "balls" bouncing back and forth along a line of
`N` worker processes, built on POSIX shared memory, `fork()`, signals, and
named pipes (FIFOs). A separate controller process lets you interactively
query per-worker statistics while the simulation runs.

## Overview

The program launches `N` worker processes (positions `1..N`) plus one
controller process, all sharing a single memory-mapped region
(`shared_data_t`) protected by a process-shared `pthread_mutex_t`.

**Important design note:** only worker process **1** actually advances the
simulation. It runs a dedicated simulation thread that owns the ball state
and steps every ball forward on each tick. Workers `2..N` do not perform
independent computation — they exist as distinct PIDs so the controller can
signal a *specific position* in the line and have that position reply with
its own stats over its own FIFO. The "line" of workers is a stats/signaling
topology, not a parallel compute pipeline: worker 1 is where all the actual
work happens.

A ball travels forward from position 1 to position `N`, then backward from
`N` to 1 — one full forward+backward trip counts as one "round." After
`ROUNDS` round trips, the ball is removed. New balls are injected (up to
`MAX_BALLS` in flight at once) until `TOTAL_BALLS` have been injected in
total.

## Build

```bash
gcc -Wall -Wextra -O2 -pthread -g -o pingpong pingpong.c -lrt
```

## Run

```bash
./pingpong N MAX_BALLS TOTAL_BALLS ROUNDS [STEP_MS]
```

| Argument | Meaning |
|---|---|
| `N` | Number of worker processes / line length (2–32) |
| `MAX_BALLS` | Max balls in flight simultaneously (1–256) |
| `TOTAL_BALLS` | Total balls injected over the run |
| `ROUNDS` | Full round trips (1→N→1) before a ball is removed |
| `STEP_MS` | Optional delay between simulation steps, default 200ms |

Example:

```bash
./pingpong 4 5 10 3
```

4 workers, up to 5 balls in flight at once, 10 balls total, each ball
removed after 3 full round trips, 200ms per step.

## Interactive controller

While the simulation runs, the controller process reads from your terminal:

- **Enter** — sends `SIGUSR1` to the current target worker (starting at
  worker 1, then cycling `2, 3, ..., N, 1, ...` on each press), then reads
  that worker's statistics back over a dedicated FIFO and prints them.
- **`q` + Enter** — exits the controller only; the simulation keeps running
  in the background until it finishes on its own.

Statistics per worker are counts of forward/backward passes through that
position, broken down by round number.

## Shared memory & synchronization

- A POSIX shared memory object (`/pingpong_shm`) holds all mutable
  state: ball array, per-worker stats, injected/removed counters, and a
  `shutdown` flag.
- A single `pthread_mutex_t`, initialized with `PTHREAD_PROCESS_SHARED`,
  guards all access to that region across every process.
- Each worker runs two threads:
  - a **signal thread**, which polls a `sig_atomic_t` flag set by a
    `SIGUSR1` handler and, when set, writes a stats snapshot to that
    worker's FIFO for the controller to read;
  - a **simulation thread** (worker 1 only), which advances every in-flight
    ball once per tick, injects new balls while under the total/in-flight
    limits, and sets `shutdown` once all balls have been injected and none
    remain active.
- Non-simulation workers just poll the `shutdown` flag on a timer and exit
  once it's set.

## Inter-process communication

- **Shared memory** carries all simulation state and statistics.
- **Named pipes** (`/tmp/pingpong_stats_<i>`), one per worker, are used
  one-shot per request: the controller opens the FIFO for reading right
  after signaling, and the worker opens it for writing once it has a
  snapshot ready.
- **`SIGUSR1`** is the trigger the controller uses to ask a specific worker
  to publish its stats immediately.

## Shutdown sequence

1. Worker 1's simulation thread sets `shutdown = 1` once all balls have
   been injected and none are still in flight.
2. All workers notice the flag on their next poll and exit.
3. `main()` waits for all worker processes via `waitpid`, then explicitly
   sets `shutdown = 1` again (covering the case where `main` gets there
   first) and sends `SIGTERM` to the controller to end it if it's still
   waiting on input.
4. Shared memory and FIFOs are unlinked and cleaned up.

## Limits

| Constant | Value | Meaning |
|---|---|---|
| `MAX_N` | 32 | Maximum worker processes |
| `MAX_BALLS_LIMIT` | 256 | Maximum balls in flight at once |
| `MAX_ROUNDS_TRACK` | 64 | Round-indexed stats buckets per worker (rounds beyond this are folded into the last bucket) |

## Known behavioral notes

- Only worker 1 performs simulation work; workers 2..N are stats/signal
  endpoints only, not independent compute units. Don't read "N worker
  processes" as "N-way parallelism" — the parallelism here is limited.
- Round counts beyond `MAX_ROUNDS_TRACK` are clamped into the last bucket
  in `record_pass`, so per-round stats become inaccurate (aggregated) past
  that point if `ROUNDS` is set very high.
- The controller's read loop blocks on `fgets(stdin)`; if stdin is not
  interactive (e.g. piped from `/dev/null`), the controller will exit on
  EOF rather than hang.