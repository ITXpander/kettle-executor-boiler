package org.itfactory.kettle.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Created by puls3 on 04/08/14.
 */
public class PrioritizedFutureTask<V> extends FutureTask<V> implements
        Runnable, Comparable<PrioritizedFutureTask<V>>{

    private int priority;
    private long completionTime = 0L;

    public int getPriority() {
        return priority;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public PrioritizedFutureTask(Callable<V> callable, int priority) {
        super(callable);
        this.priority = priority;
    }

    public PrioritizedFutureTask(Runnable runnable, V result, int priority) {
        super(runnable, result);
        this.priority = priority;
    }

    @Override
    protected void done() {
        this.completionTime = System.currentTimeMillis();
    }

    @Override
    public int compareTo(PrioritizedFutureTask<V> o) {
        return this.priority - o.priority;
    }

}
