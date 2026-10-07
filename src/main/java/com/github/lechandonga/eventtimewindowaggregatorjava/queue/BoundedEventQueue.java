package com.github.lechandonga.eventtimewindowaggregatorjava.queue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Bounded buffer between WAL ingress and the single consumer thread.
 *
 * <p>The queue is deliberately offer-based: when backlog reaches capacity
 * {@link #tryEnqueue} returns false immediately instead of blocking HTTP
 * threads. Ingress then answers 503 and bumps {@code rejectedFull}, so the
 * overload is explicit and observable.
 */
public class BoundedEventQueue<E> {

    private final LinkedBlockingQueue<E> queue;
    private final int capacity;
    private volatile long rejectedFullCount;

    public BoundedEventQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public boolean tryEnqueue(E element) {
        if (queue.offer(element)) {
            return true;
        }
        rejectedFullCount++;
        return false;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    public long rejectedFullCount() {
        return rejectedFullCount;
    }
}
