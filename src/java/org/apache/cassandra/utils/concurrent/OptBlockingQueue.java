package org.apache.cassandra.utils.concurrent;

/**
 * A NonBlockingQueue that supports a blocking take(), i.e. all operations are non-blocking, except take() which
 * blocks until there is something to remove from the queue.
 *
 * This is a very simple extension of NonBlockingQueue, that overrides all appending methods to signal a WaitQueue
 * when the queue is appended to.
 *
 * @param <V>
 */
public class OptBlockingQueue<V> extends NonBlockingQueue<V> implements Iterable<V>
{

    final WaitQueue notEmpty = new WaitQueue();

    /**
     * Add the provided item to the end of the queue
     *
     * @param append
     */
    public void append(V append)
    {
        super.append(append);
        notEmpty.signal();
    }

    /**
     * Add <code>append</code> to the end of the queue iff the end of the queue is currently
     * <code>expectedTail</code>. Note that this works even if the queue is now empty but the last item
     * prior to the queue being empty was <code>expectedTail</code>.
     *
     * @param expectedTail the last item we expect to be in the queue, or the last item returned if empty
     * @param append the item to add
     * @return true iff success
     */
    public boolean appendIfTail(V expectedTail, V append)
    {
        if (super.appendIfTail(expectedTail, append))
        {
            notEmpty.signal();
            return true;
        }
        return false;
    }

    /**
     * blocks until an item can be returned from the queue
     *
     * @return
     * @throws InterruptedException
     */
    public V advanceOrWait() throws InterruptedException
    {
        while (true)
        {
            Node<V> r = advanceNode();
            if (r != null)
                return r.value;
            WaitQueue.Signal signal = notEmpty.register();
            if ((r = advanceNode()) != null)
            {
                signal.cancel();
                return r.value;
            }
            signal.await();
        }
    }
}
