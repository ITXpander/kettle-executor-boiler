package org.itfactory.kettle.concurrent;

import java.util.concurrent.*;

/**
 * Created by puls3 on 04/08/14.
 */
public class PrioritizedThreadPoolExecutor extends ThreadPoolExecutor {

    public PrioritizedThreadPoolExecutor(int corePoolSize,
                                         int maximumPoolSize,
                                         long keepAliveTime,
                                         TimeUnit unit,
                                         PriorityBlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public PrioritizedThreadPoolExecutor(int corePoolSize,
                                         int maximumPoolSize,
                                         long keepAliveTime,
                                         TimeUnit unit,
                                         PriorityBlockingQueue<Runnable> workQueue,
                                         ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public PrioritizedThreadPoolExecutor(int corePoolSize,
                                         int maximumPoolSize,
                                         long keepAliveTime,
                                         TimeUnit unit,
                                         PriorityBlockingQueue<Runnable> workQueue,
                                         RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public <T> Future<T> submit(Runnable task, T result, int priority) {
        if (task == null)
            throw new NullPointerException();
        RunnableFuture<T> futureTask = this.newTaskFor(task, result, priority);
        this.execute(futureTask);
        return futureTask;
    }

    public <T> Future<T> submit(Callable<T> task, int priority) {
        if (task == null)
            throw new NullPointerException();
        RunnableFuture<T> futureTask = this.newTaskFor(task, priority);
        this.execute(futureTask);
        return futureTask;
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value,
                                               int priority) {
        return new PrioritizedFutureTask<T>(runnable, value, priority);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable,
                                               int priority) {
        return new PrioritizedFutureTask<T>(callable, priority);
    }
}
