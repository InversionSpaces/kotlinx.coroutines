package kotlinx.coroutines

import kotlinx.coroutines.testing.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import kotlin.test.*

class DefaultVirtualThreadDispatcherTest : TestBase() {
    @Test
    fun testCompletionDrainsAcceptedTasksOnTheSameThread() {
        val job = Job()
        val started = CompletableFuture<Thread>()
        val release = CountDownLatch(1)
        val observed = mutableListOf<Pair<Int, Thread>>()
        DefaultVirtualThreadDispatcher.dispatch(job, Runnable {
            started.complete(Thread.currentThread())
            release.await()
        })
        val worker = started.get(10, TimeUnit.SECONDS)
        try {
            repeat(100) { index ->
                DefaultVirtualThreadDispatcher.dispatch(job, Runnable {
                    observed += index to Thread.currentThread()
                })
            }
            job.complete()
        } finally {
            job.complete()
            release.countDown()
        }
        joinWorker(worker)
        assertEquals((0 until 100).toList(), observed.map { it.first })
        observed.forEach { assertSame(worker, it.second) }
    }

    @Test
    fun testFailingTaskReleasesReservation() {
        val job = Job()
        val failure = IllegalStateException("Test failure")
        val reported = CompletableFuture<Throwable>()
        val worker = CompletableFuture<Thread>()
        val context = job + CoroutineExceptionHandler { _, exception -> reported.complete(exception) }
        DefaultVirtualThreadDispatcher.dispatch(context, Runnable {
            worker.complete(Thread.currentThread())
            throw failure
        })
        job.complete()
        joinWorker(worker.get(10, TimeUnit.SECONDS))
        assertSame(failure, reported.get(10, TimeUnit.SECONDS))
    }

    @Test
    fun testConcurrentDispatchAndCompletion() {
        val producers = 4
        val iterations = 1_000
        val tasksPerProducer = 4
        val jobs = List(iterations) { Job() }
        val executions = AtomicIntegerArray(iterations * producers * tasksPerProducer)
        val executed = CountDownLatch(executions.length())
        val workers = ConcurrentHashMap.newKeySet<Thread>()
        val barrier = CyclicBarrier(producers + 1)
        val executor = Executors.newFixedThreadPool(producers)
        try {
            val submissions = List(producers) { producer ->
                executor.submit {
                    repeat(iterations) { iteration ->
                        barrier.await(10, TimeUnit.SECONDS)
                        repeat(tasksPerProducer) { task ->
                            val index = (iteration * producers + producer) * tasksPerProducer + task
                            DefaultVirtualThreadDispatcher.dispatch(jobs[iteration], Runnable {
                                workers.add(Thread.currentThread())
                                executions.incrementAndGet(index)
                                executed.countDown()
                            })
                        }
                        barrier.await(10, TimeUnit.SECONDS)
                    }
                }
            }
            repeat(iterations) { iteration ->
                // Exercise both already-completed jobs and completion racing with submissions.
                if (iteration % 2 == 0) jobs[iteration].complete()
                barrier.await(10, TimeUnit.SECONDS)
                jobs[iteration].complete()
                barrier.await(10, TimeUnit.SECONDS)
            }
            submissions.forEach { it.get(10, TimeUnit.SECONDS) }
            assertTrue(executed.await(10, TimeUnit.SECONDS), "Some dispatched tasks were lost")
            workers.forEach { joinWorker(it) }
            repeat(executions.length()) { index ->
                assertEquals(1, executions.get(index), "Task $index must execute exactly once")
            }
        } finally {
            jobs.forEach { it.complete() }
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
        }
    }

    private fun joinWorker(worker: Thread) {
        worker.join(10_000)
        assertFalse(worker.isAlive, "Completed job's worker did not retire")
    }
}
