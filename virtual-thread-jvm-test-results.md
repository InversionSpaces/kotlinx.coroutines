# Dedicated virtual-thread dispatcher: JVM test results

Measured on 2026-09-15 against the `1.10.1` release baseline with JDK 25. Every batch was run in a
separate Gradle invocation under a five-minute external timeout with a further 15-second kill grace period.
The previous suspension-blocking experiment and its report remain available in Git history.

## Results

| Batch | Tests reported | Passed | Failed | Skipped | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Channels | 473 | 471 | 2 | 0 | Completed in 8 seconds |
| Flow | 703 | 702 | 0 | 1 | Completed in 10 seconds |
| Select and synchronization | 100 | 100 | 0 | 0 | Completed in 7 seconds |
| Scheduler, internals, interop, time, and guides | incomplete | incomplete | at least 6 | unknown | External timeout after 5 minutes |
| Top-level core API | incomplete | incomplete | at least 1 | unknown | Stopped while investigating a hang |

The three completed batches account for 1,276 tests: 1,273 passed, 2 failed, and 1 was skipped. No
flow, select, or synchronization test failed. In particular, the flow and channel deadlocks seen in the
old implementation did not recur.

## Completed failures

### Ticker virtual-time ordering

Both parameterizations of
`kotlinx.coroutines.channels.TickerChannelCommonTest.testComplexOperator` failed:

- `FIXED_PERIOD`: expected `[2.0, 5.0, 7.0]`, observed
  `[1.0, NaN, 2.0, NaN, 3.0, NaN, 4.0, 5.0, 6.0, 7.0]`.
- `FIXED_DELAY`: expected `[2.0, 5.0, 7.0]`, observed
  `[1.0, NaN, 2.0, 3.5, NaN, 5.0, NaN, 6.0, NaN, 7.0, NaN]`.

The test uses `GlobalScope.produce`, so the producer and time-window coroutine now receive independent
Default-dispatcher workers. Their concurrent progress no longer follows the cooperative ordering assumed by
the virtual-time test. Other ticker tests passed.

### Limited-parallelism diagnostics

`kotlinx.coroutines.DispatchersToStringTest.testLimitedParallelism` failed. The scheduler-backed Default
dispatcher returns itself when the requested parallelism is at least its core-pool size. The virtual-thread
dispatcher inherits the generic `LimitedDispatcher` implementation and therefore produces a different
diagnostic string. This also reveals a design gap: generic `LimitedDispatcher` schedules its own worker loops
with an empty coroutine context, so it does not preserve the one-virtual-thread-per-job routing guarantee.

### Guide output

The runtime batch reported output-comparison failures in:

- `DispatcherGuideTest.testExampleContext01`
- `DispatcherGuideTest.testExampleContext09`
- `DispatcherGuideTest.testExampleContext10`
- `DispatcherGuideTest.testExampleContext11`
- `ExceptionsGuideTest.testExampleExceptions01`
- `FlowGuideTest.testExampleFlow15`

Most expected `DefaultDispatcher-worker-N` thread names. The virtual-thread executor currently creates unnamed
threads. `DispatcherGuideTest.testExampleContext11` additionally expects `yield` to move a coroutine from
worker 1 to worker 2; retaining one worker for the coroutine intentionally changes that output. Context 10 is
also sensitive to concurrent output ordering.

## Hangs and likely causes

### Top-level core batch

The batch stopped producing output around `ExecutorsTest`. A concrete indefinite-wait path exists in
`ExecutorsTest.testDefaultDispatcherToExecutor`: its task first asserts that the thread name begins with
`DefaultDispatcher`, and only then decrements a latch. The unnamed virtual thread fails the assertion, exits
before decrementing the latch, and leaves the test thread waiting forever.

Giving all dispatcher-created virtual threads stable `DefaultDispatcher-worker-N` names, including tasks
submitted through `Dispatchers.Default.asExecutor()`, should remove this hang and several guide failures.

### Scheduler/runtime batch

The batch timed out after completing `CoroutineDispatcherTest`. The next scheduler tests include
`CoroutineSchedulerOversubscriptionTest`, whose invariants assume a bounded CPU worker pool, local queues, and
work stealing. A dedicated virtual thread for every job intentionally violates those assumptions. Its stress
case repeats the scenario 1,000 times, so it may either fail through oversubscription or run beyond the batch
limit. It must be isolated before assigning a single exact hanging test.

Scheduler-specific tests are not all meaningful for this dispatcher: `Dispatchers.Default` no longer exposes
`CoroutineScheduler.Worker` threads, CPU permits, local queues, or stealing. Tests of public coroutine behavior
remain meaningful; tests of the old scheduler implementation should continue targeting `DefaultScheduler`
directly or be classified as incompatible with this experiment.

## Implementation risks found during review

- `ExecutorCoroutineDispatcher.executor` currently exposes the raw virtual-thread executor. Calls through
  `Dispatchers.Default.asExecutor()` therefore bypass job routing and thread naming.
- The generic `limitedParallelism` view loses the originating `Job` when it dispatches its internal worker loop,
  so continuation segments are not guaranteed to return to the coroutine's dedicated virtual thread.
- `shutdownDefault()` shuts down the shared executor while completed and suspended coroutine workers may still
  be waiting on their mailboxes. Worker shutdown and dispatcher shutdown need an explicit lifecycle policy.
- Tests that assert a bounded Default parallelism cannot pass under an unrestricted one-worker-per-coroutine
  model without redefining whether the bound applies to active continuation segments or to virtual threads.

## Next fixes to validate

1. Wrap the exposed executor and name every virtual thread `DefaultDispatcher-worker-N`.
2. Implement a job-aware limited-parallelism view, or explicitly reject limited-parallelism for this
   experimental dispatcher.
3. Add deterministic worker cleanup and shutdown tests.
4. Re-run the two incomplete batches in per-class groups with short external timeouts after the naming fix.
