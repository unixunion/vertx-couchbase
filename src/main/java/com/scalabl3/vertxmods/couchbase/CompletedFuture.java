package com.scalabl3.vertxmods.couchbase;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by keghol on 5/25/14.
 *
 */

public class CompletedFuture<T> implements Future<T> {
    private final T result;

    public CompletedFuture(final T result) {
        this.result = result;
    }

    @Override
    public boolean cancel(final boolean b) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return this.result;
    }

    @Override
    public T get(final long l, final TimeUnit timeUnit) throws InterruptedException, ExecutionException {
        return get();
    }
}
