package kotlinx.coroutines

import java.util.concurrent.*
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
        private val queue = LinkedBlockingQueue<Task>()
        private val wakeup = Task(EmptyCoroutineContext, Runnable {})

        @Volatile
        private var completed = false

        private var accepting = true

        fun start() {
            job.invokeOnCompletion {
                completed = true
                queue.offer(wakeup)
            }
            executorService.execute(this)
        }

        fun enqueue(task: Task): Boolean = synchronized(this) {
            if (!accepting) return false
            queue.offer(task)
            true
        }

        override fun run() {
            while (true) {
                val task = queue.take()
                if (task !== wakeup) {
                    try {
                        task.block.run()
                    } catch (failure: Throwable) {
                        handleCoroutineException(task.context, failure)
                    }
                }

                if (completed && queue.isEmpty() && stopAccepting()) {
                    workers.remove(job, this)
                    return
                }
            }
        }

        private fun stopAccepting(): Boolean = synchronized(this) {
            if (!completed || queue.isNotEmpty()) return false
            accepting = false
            true
        }
    }
}
