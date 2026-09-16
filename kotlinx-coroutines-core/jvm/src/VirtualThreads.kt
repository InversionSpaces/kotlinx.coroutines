package kotlinx.coroutines

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import kotlin.coroutines.*

/**
 * A dispatcher that gives each coroutine job a dedicated virtual-thread worker.
 *
 * Suspension still uses the regular Kotlin continuation protocol. The worker's JVM stack unwinds and the
 * virtual thread waits for the next dispatched continuation, so all dispatched segments for one job retain
 * thread identity without changing suspension or resumption semantics.
 */
internal object DefaultVirtualThreadDispatcher : ExecutorCoroutineDispatcher(), CoroutineStartOnDispatcher {
    private val executorService =
        Executors::class.java.getMethod("newVirtualThreadPerTaskExecutor").invoke(null) as ExecutorService

    private val workers = ConcurrentHashMap<Job, CoroutineWorker>()

    override val executor: Executor
        get() = executorService

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        val job = context[Job]
        if (job == null) {
            executorService.execute(block)
            return
        }

        val task = Task(context, block)
        while (true) {
            var created = false
            val worker = workers.computeIfAbsent(job) {
                created = true
                CoroutineWorker(job)
            }
            if (worker.enqueue(task)) {
                if (created) worker.start()
                return
            }
            workers.remove(job, worker)
        }
    }

    internal fun shutdownDefault() {
        executorService.shutdown()
    }

    override fun close() {
        throw UnsupportedOperationException("Dispatchers.Default cannot be closed")
    }

    override fun toString(): String = "Dispatchers.Default"

    private class Task(val context: CoroutineContext, val block: Runnable)

    private class CoroutineWorker(private val job: Job) : Runnable {
        private val queue = ConcurrentLinkedQueue<Task>()

        private val completed = AtomicBoolean(false)
        private val accepting = AtomicBoolean(true)

        @Volatile
        private var thread: Thread? = null

        fun start() {
            job.invokeOnCompletion {
                completed.set(true)
                signal()
            }
            executorService.execute(this)
        }

        fun enqueue(task: Task): Boolean {
            if (!accepting.get()) return false
            queue.offer(task)
            if (!accepting.get()) {
                queue.remove(task)
                return false
            }
            signal()
            return true
        }

        override fun run() {
            thread = Thread.currentThread()
            while (true) {
                val task = queue.poll()
                if (task != null) {
                    try {
                        task.block.run()
                    } catch (failure: Throwable) {
                        handleCoroutineException(task.context, failure)
                    }
                    continue
                }

                if (completed.get() && stopAccepting()) {
                    workers.remove(job, this)
                    return
                }

                LockSupport.park(this)
            }
        }

        private fun stopAccepting(): Boolean {
            if (!completed.get() || queue.isNotEmpty()) return false
            return accepting.compareAndSet(true, false)
        }

        private fun signal() {
            thread?.let(LockSupport::unpark)
        }
    }
}
