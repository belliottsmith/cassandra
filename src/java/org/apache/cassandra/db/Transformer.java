/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A class for applying multiple transformations to row or partitions iterators.
 * The intention is to reduce the cognitive (and cpu) burden of deep nested method calls, by accumulating a stack of
 * transformations in one outer iterator.
 *
 * This is achieved by detecting the transformation of an already transformed iterator, and simply pushing
 * the new transformation onto the top of the stack of existing ones.
 *
 * There is a degree of shared transformationality between both RowIterator and PartitionIterator, encapsulated in the
 * Abstract class. This is then extended by each of BasePartitions and BaseRows, which perform all of the heavy
 * lifting for each of their class of iterator, with each of their extensions just providing the correct front-end
 * interface implementations.
 *
 * Filtering is performed as another transformation that is pushed onto the stack, but can only be created internally
 * to this class, as it performs the transition from Unfiltered -> Filtered, which results in the swap of the front-end
 * interface (from UnfilteredRowIterator/UnfilteredPartitionIterator -> RowIterator/PartitionIterator).
 *
 * This permits us to use the exact same iterator for the entire stack.
 *
 * Advanced use permits swapping the contents of the transformer once it has been exhausted; this is useful
 * for concatenation (inc. short-read protection). When such a transformation is attached, the current snapshot of
 * the transformation stack is saved, and when it is *executed* this part of the stack is removed (leaving only those
 * transformations that were applied later, i.e. on top of the concatenated stream). If the new iterator is itself
 * a transformer, its contents are inserted directly into this stream, and its transformation stack prefixed to our remaining
 * stack. This sounds complicated, but the upshot is quite straightforward: a single point of control flow even across
 * multiple concatenated streams of information.
 *
 * Note that only the normal iterator stream is affected by a moreContents; the caller must ensure that everything
 * else is the same for all concatenated streams. Notably static rows, deletions, metadatas are all retained from
 * the first iterator.
 */
public class Transformer
{

    /** DECLARATIONS **/

    /**
     * We have a single common superclass for all Transformations to make implementation efficient.
     * we have a shared stack for all transformations, and can share the same transformation across partition and row
     * iterators, reducing garbage. Internal code is also simplified by always having a basic no-op implementation to invoke.
     *
     * Only the necessary methods need be overridden. Early termination is provided by invoking the method's stop or stopInPartition
     * methods, rather than having their own abstract method to invoke, as this is both more efficient and simpler to reason about.
     */
    public static abstract class Transformation<I extends BaseRowIterator<?>>
    {
        // internal methods for StoppableTransformation only
        void attachTo(BasePartitions partitions) { }
        void attachTo(BaseRows rows) { }

        /**
         * Run on the close of any (logical) partitions iterator this function was applied to
         *
         * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
         * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
         * is refilled with MoreContents, for instance, the iterator may outlive this function
         */
        protected void onClose() { }

        /**
         * Run on the close of any (logical) rows iterator this function was applied to
         *
         * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
         * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
         * is refilled with MoreContents, for instance, the iterator may outlive this function
         */
        protected void onPartitionClose() { }

        /**
         * Applied to any rows iterator (partition) we encounter in a partitions iterator
         */
        protected I applyToPartition(I partition)
        {
            return partition;
        }

        /**
         * Applied to any row we encounter in a rows iterator
         */
        protected Row applyToRow(Row row)
        {
            return row;
        }

        /**
         * Applied to any RTM we encounter in a rows/unfiltered iterator
         */
        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return marker;
        }

        /**
         * Applied to the static row of any rows iterator.
         *
         * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
         * the static data for such iterators is all expected to be equal
         */
        protected Row applyToStatic(Row row)
        {
            return row;
        }

        /**
         * Applied to the partition-level deletion of any rows iterator.
         *
         * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
         * the static data for such iterators is all expected to be equal
         */
        protected DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            return deletionTime;
        }
    }

    // A Transformation that can stop an iterator earlier than its natural exhaustion
    public static abstract class StoppingTransformation<I extends BaseRowIterator<?>> extends Transformation<I>
    {
        private BaseIterator.Stop stop;
        private BaseIterator.Stop stopInPartition;

        /**
         * If invoked by a subclass, any partitions iterator this transformation has been applied to will terminate
         * after any currently-processing item is returned, as will any row/unfiltered iterator
         */
        @DontInline
        protected void stop()
        {
            if (stop != null)
                stop.isSignalled = true;
            stopInPartition();
        }

        /**
         * If invoked by a subclass, any rows/unfiltered iterator this transformation has been applied to will terminate
         * after any currently-processing item is returned
         */
        @DontInline
        protected void stopInPartition()
        {
            if (stopInPartition != null)
                stopInPartition.isSignalled = true;
        }

        @Override
        protected void attachTo(BasePartitions partitions)
        {
            assert this.stop == null;
            this.stop = partitions.stop;
        }

        @Override
        protected void attachTo(BaseRows rows)
        {
            assert this.stopInPartition == null;
            this.stopInPartition = rows.stop;
        }

        @Override
        protected void onClose()
        {
            stop = null;
        }

        @Override
        protected void onPartitionClose()
        {
            stopInPartition = null;
        }
    }

    /**
     * An interface for providing new partitions for a partitions iterator.
     *
     * The new contents are produced as a normal arbitrary PartitionIterator or UnfilteredPartitionIterator (as appropriate)
     *
     * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
     * new contents as the new source.
     *
     * If the new source is itself a product of any transformations, the two transforming iterators are merged
     * so that control flow always occurs at the outermost point
     */
    public static interface MorePartitions<I extends BasePartitionIterator<?>> extends MoreContents<I> {}

    /**
     * An interface for providing new row contents for a partition.
     *
     * The new contents are produced as a normal arbitrary RowIterator or UnfilteredRowIterator (as appropriate),
     * with matching staticRow, partitionKey and partitionLevelDeletion.
     *
     * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
     * new contents as the new source.
     *
     * If the new source is itself a product of any transformations, the two transforming iterators are merged
     * so that control flow always occurs at the outermost point
     */
    public static interface MoreRows<I extends BaseRowIterator<?>> extends MoreContents<I> {}

    // a shared internal interface, that is hidden to provide type-safety to the user
    private static interface MoreContents<I>
    {
        public abstract I moreContents();
    }

    /** STATIC TRANSFORMATION ACCESSORS **/

    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Transformation<? super UnfilteredRowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static UnfilteredPartitionIterator extend(UnfilteredPartitionIterator iterator, MorePartitions<? super UnfilteredPartitionIterator> more)
    {
        return add(mutable(iterator), more);
    }
    private static UnfilteredPartitions mutable(UnfilteredPartitionIterator iterator)
    {
        return iterator instanceof UnfilteredPartitions
               ? (UnfilteredPartitions) iterator
               : new UnfilteredPartitions(iterator);
    }

    public static PartitionIterator apply(PartitionIterator iterator, Transformation<? super RowIterator> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static PartitionIterator extend(PartitionIterator iterator, MorePartitions<? super PartitionIterator> more)
    {
        return add(mutable(iterator), more);
    }
    private static FilteredPartitions mutable(PartitionIterator iterator)
    {
        return iterator instanceof FilteredPartitions
               ? (FilteredPartitions) iterator
               : new FilteredPartitions(iterator);
    }

    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more)
    {
        return add(mutable(iterator), more);
    }
    private static UnfilteredRows mutable(UnfilteredRowIterator iterator)
    {
        return iterator instanceof UnfilteredRows
               ? (UnfilteredRows) iterator
               : new UnfilteredRows(iterator);
    }

    public static RowIterator apply(RowIterator iterator, Transformation<?> transformation)
    {
        return add(mutable(iterator), transformation);
    }
    public static RowIterator extend(RowIterator iterator, MoreRows<? super RowIterator> more)
    {
        return add(mutable(iterator), more);
    }
    private static FilteredRows mutable(RowIterator iterator)
    {
        return iterator instanceof FilteredRows
               ? (FilteredRows) iterator
               : new FilteredRows(iterator);
    }

    private static <E extends BaseIterator> E add(E to, Transformation add)
    {
        to.add(add);
        return to;
    }
    private static <E extends BaseIterator> E add(E to, MoreContents add)
    {
        to.add(add);
        return to;
    }

    /** FILTRATION **/

    /**
     * Filter any RangeTombstoneMarker from the iterator's iterators, transforming it into a PartitionIterator.
     */
    public static PartitionIterator filter(UnfilteredPartitionIterator iterator, int nowInSecs)
    {
        Filter filter = new Filter(!iterator.isForThrift(), nowInSecs);
        if (iterator instanceof UnfilteredPartitions)
            return new FilteredPartitions(filter, (UnfilteredPartitions) iterator);
        return new FilteredPartitions(iterator, filter);
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator, transforming it into a RowIterator.
     */
    public static RowIterator filter(UnfilteredRowIterator iterator, int nowInSecs)
    {
        return new Filter(false, nowInSecs).applyToPartition(iterator);
    }

    /**
     * Simple transformation that filters out range tombstone markers, and purges tombstones
     */
    private static final class Filter extends Transformation
    {
        private final boolean filterEmpty; // generally maps to !isForThrift, but also false for direct row filtration
        private final int nowInSec;
        public Filter(boolean filterEmpty, int nowInSec)
        {
            this.filterEmpty = filterEmpty;
            this.nowInSec = nowInSec;
        }

        public RowIterator applyToPartition(BaseRowIterator iterator)
        {
            FilteredRows filtered =  iterator instanceof UnfilteredRows
                                     ? new FilteredRows(this, (UnfilteredRows) iterator)
                                     : new FilteredRows((UnfilteredRowIterator) iterator, this);

            if (filterEmpty && closeIfEmpty(filtered))
                return null;

            return filtered;
        }

        public Row applyToStatic(Row row)
        {
            if (row.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            row = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
            return row == null ? Rows.EMPTY_STATIC_ROW : row;
        }

        public Row applyToRow(Row row)
        {
            return row.purge(DeletionPurger.PURGE_ALL, nowInSec);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return null;
        }

        private static boolean closeIfEmpty(BaseRowIterator<?> iter)
        {
            if (iter.isEmpty())
            {
                iter.close();
                return true;
            }
            return false;
        }
    }

     /* ROWS IMPLEMENTATION */

    private static final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
    {
        private DeletionTime partitionLevelDeletion;

        public UnfilteredRows(UnfilteredRowIterator input)
        {
            super(input);
            partitionLevelDeletion = input.partitionLevelDeletion();
        }

        void add(Transformation add)
        {
            super.add(add);
            partitionLevelDeletion = add.applyToDeletion(partitionLevelDeletion);
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public EncodingStats stats()
        {
            return input.stats();
        }

        public boolean isEmpty()
        {
            return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
        }
    }

    private static final class FilteredRows extends BaseRows<Row, BaseRowIterator<?>> implements RowIterator
    {
        FilteredRows(RowIterator input)
        {
            super(input);
        }

        FilteredRows(UnfilteredRowIterator input, Filter filter)
        {
            super(input);
            add(filter);
        }

        FilteredRows(Filter filter, UnfilteredRows input)
        {
            super(input);
            add(filter);
        }

        public boolean isEmpty()
        {
            return staticRow().isEmpty() && !hasNext();
        }
    }

    public static abstract class BaseRows<R extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>> extends BaseIterator<Unfiltered, I, R> implements BaseRowIterator<R>
    {
        private Row staticRow;

        public BaseRows(I input)
        {
            super(input);
            staticRow = input.staticRow();
        }

        // swap parameter order to avoid casting errors
        BaseRows(BaseRows<?, ? extends I> copyFrom)
        {
            super(copyFrom);
            staticRow = copyFrom.staticRow;
        }

        public CFMetaData metadata()
        {
            return input.metadata();
        }

        public boolean isReverseOrder()
        {
            return input.isReverseOrder();
        }

        public PartitionColumns columns()
        {
            return input.columns();
        }

        public DecoratedKey partitionKey()
        {
            return input.partitionKey();
        }

        public Row staticRow()
        {
            return staticRow;
        }

        protected Throwable runOnClose(int length)
        {
            Throwable fail = null;
            Transformation[] fs = stack;
            for (int i = 0 ; i < length ; i++)
            {
                try
                {
                    fs[i].onPartitionClose();
                }
                catch (Throwable t)
                {
                    fail = merge(fail, t);
                }
            }
            return fail;
        }


        // *********** /Begin Core Debug Point

        @Override
        protected Unfiltered applyOne(Unfiltered value, Transformation transformation)
        {
            return value == null
                   ? null
                   : value instanceof Row
                     ? transformation.applyToRow((Row) value)
                     : transformation.applyToMarker((RangeTombstoneMarker) value);
        }

        @Override
        void add(Transformation transformation)
        {
            transformation.attachTo(this);
            super.add(transformation);

            // transform any existing data
            staticRow = transformation.applyToStatic(staticRow);
            next = applyOne(next, transformation);
        }

        public final boolean hasNext()
        {
            Stop stop = this.stop;
            while (this.next == null)
            {
                Transformation[] fs = stack;
                int len = length;

                while (!stop.isSignalled && input.hasNext())
                {
                    Unfiltered next = input.next();

                    if (next.isRow())
                    {
                        Row row = (Row) next;
                        for (int i = 0 ; row != null && i < len ; i++)
                            row = fs[i].applyToRow(row);
                        next = row;
                    }
                    else
                    {
                        RangeTombstoneMarker rtm = (RangeTombstoneMarker) next;
                        for (int i = 0 ; rtm != null && i < len ; i++)
                            rtm = fs[i].applyToMarker(rtm);
                        next = rtm;
                    }

                    if (next != null)
                    {
                        this.next = next;
                        return true;
                    }
                }

                if (stop.isSignalled || !hasMoreContents())
                    return false;
            }
            return true;
        }



        // *********** /End Core Debug Point
    }

    /* PARTITIONS IMPLEMENTATION */

    private static final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator
    {
        final boolean isForThrift;

        // wrap an iterator for transformation
        public UnfilteredPartitions(UnfilteredPartitionIterator input)
        {
            super(input);
            this.isForThrift = input.isForThrift();
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }

        public CFMetaData metadata()
        {
            return input.metadata();
        }
    }

    private static final class FilteredPartitions extends BasePartitions<RowIterator, BasePartitionIterator<?>> implements PartitionIterator
    {
        // wrap basic iterator for transformation
        FilteredPartitions(PartitionIterator input)
        {
            super(input);
        }

        // wrap basic unfiltered iterator for transformation, applying filter as first transformation
        FilteredPartitions(UnfilteredPartitionIterator input, Filter filter)
        {
            super(input);
            add(filter);
        }

        // copy from an UnfilteredPartitions, applying a filter to convert it
        FilteredPartitions(Filter filter, UnfilteredPartitions copyFrom)
        {
            super(copyFrom);
            add(filter);
        }
    }

    public static abstract class BasePartitions<R extends BaseRowIterator<?>,
                                                I extends BasePartitionIterator<? extends BaseRowIterator<?>>>
    extends BaseIterator<BaseRowIterator<?>, I, R>
    implements BasePartitionIterator<R>
    {
        public BasePartitions(I input)
        {
            super(input);
        }

        BasePartitions(BasePartitions<?, ? extends I> copyFrom)
        {
            super(copyFrom);
        }


        // *********** /Begin Core Debug Point



        protected BaseRowIterator<?> applyOne(BaseRowIterator<?> value, Transformation transformation)
        {
            return value == null ? null : transformation.applyToPartition(value);
        }

        void add(Transformation transformation)
        {
            transformation.attachTo(this);
            super.add(transformation);
            next = applyOne(next, transformation);
        }

        protected Throwable runOnClose(int length)
        {
            Throwable fail = null;
            Transformation[] fs = stack;
            for (int i = 0 ; i < length ; i++)
            {
                try
                {
                    fs[i].onClose();
                }
                catch (Throwable t)
                {
                    fail = merge(fail, t);
                }
            }
            return fail;
        }

        public final boolean hasNext()
        {
            BaseRowIterator<?> next = null;
            try
            {

                Stop stop = this.stop;
                while (this.next == null)
                {
                    Transformation[] fs = stack;
                    int len = length;

                    while (!stop.isSignalled && input.hasNext())
                    {
                        next = input.next();
                        for (int i = 0 ; next != null & i < len ; i++)
                            next = fs[i].applyToPartition(next);

                        if (next != null)
                        {
                            this.next = next;
                            return true;
                        }
                    }

                    if (stop.isSignalled || !hasMoreContents())
                        return false;
                }
                return true;

            }
            catch (Throwable t)
            {
                if (next != null)
                    Throwables.close(t, Collections.singleton(next));
                throw t;
            }
        }

        // *********** /End Core Debug Point
    }

    /** SHARED IMPLEMENTATION **/

    private abstract static class BaseIterator<V, I extends CloseableIterator<? extends V>, O extends V> extends Stack implements AutoCloseable, Iterator<O>
    {
        I input;
        V next;
        Stop stop; // applies at the end of the current next()

        static class Stop
        {
            // TODO: consider moving "next" into here, so that a stop() when signalled outside of a function call (e.g. in attach)
            // can take effect immediately; this doesn't seem to be necessary at the moment, but it might cause least surprise in future
            boolean isSignalled;
        }

        // responsibility for initialising next lies with the subclass
        private BaseIterator(BaseIterator<? extends V, ? extends I, ?> copyFrom)
        {
            super(copyFrom);
            this.input = copyFrom.input;
            this.next = copyFrom.next;
            this.stop = copyFrom.stop;
        }

        private BaseIterator(I input)
        {
            this.input = input;
            this.stop = new Stop();
        }

        public final void close()
        {
            Throwable fail = runOnClose(length);
            if (next instanceof AutoCloseable)
            {
                try { ((AutoCloseable) next).close(); }
                catch (Throwable t) { fail = merge(fail, t); }
            }
            try { input.close(); }
            catch (Throwable t) { fail = merge(fail, t); }
            maybeFail(fail);
        }

        /**
         * run the corresponding runOnClose method for the first length transformations.
         *
         * used in hasMoreContents to close the methods preceding the MoreContents
         */
        protected abstract Throwable runOnClose(int length);

        /**
         * apply the relevant method from the transformation to the value.
         *
         * used in hasMoreContents to apply the functions that follow the MoreContents
         */
        protected abstract V applyOne(V value, Transformation transformation);

        public final O next()
        {
            if (next == null && !hasNext())
                throw new NoSuchElementException();

            O next = (O) this.next;
            this.next = null;
            return next;
        }

        protected final boolean hasMoreContents()
        {
            return moreContents.length > 0 && tryGetMoreContents();
        }

        @DontInline
        private boolean tryGetMoreContents()
        {
            for (int i = 0 ; i < moreContents.length ; i++)
            {
                MoreContentsHolder holder = moreContents[i];
                MoreContents provider = holder.moreContents;
                I newContents = (I) provider.moreContents();
                if (newContents == null)
                    continue;

                input.close();
                input = newContents;
                Stack prefix = EMPTY;
                if (newContents instanceof BaseIterator)
                {
                    // we're refilling with transformed contents, so swap in its internals directly
                    // TODO: ensure that top-level data is consistent. i.e. staticRow, partitionlevelDeletion etc are same?
                    BaseIterator abstr = (BaseIterator) newContents;
                    prefix = abstr;
                    input = (I) abstr.input;
                    next = apply((V) abstr.next, holder.length); // must apply all remaining functions to the next, if any
                }

                // since we're truncating our transformation stack to only those occurring after the extend transformation
                // we have to run any prior runOnClose methods
                maybeFail(runOnClose(holder.length));
                refill(prefix, holder, i);

                if (next != null || input.hasNext())
                    return true;

                i = -1;
            }
            return false;
        }

        // apply the functions [from..length)
        private V apply(V next, int from)
        {
            while (next != null & from < length)
                next = applyOne(next, stack[from++]);
            return next;
        }
    }

    private static class Stack
    {
        static final Stack EMPTY = new Stack();

        Transformation[] stack;
        int length; // number of used stack entries
        MoreContentsHolder[] moreContents; // stack of more contents providers (if any; usually zero or one)

        // an internal placeholder for a MoreContents, storing the associated stack length at time it was applied
        static class MoreContentsHolder
        {
            final MoreContents moreContents;
            int length;
            private MoreContentsHolder(MoreContents moreContents, int length)
            {
                this.moreContents = moreContents;
                this.length = length;
            }
        }

        Stack()
        {
            stack = new Transformation[0];
            moreContents = new MoreContentsHolder[0];
        }

        Stack(Stack copy)
        {
            stack = copy.stack;
            length = copy.length;
            moreContents = copy.moreContents;
        }

        // push the provided transformation onto any matching stack types
        void add(Transformation add)
        {
            if (length == stack.length)
                stack = resize(stack);
            stack[length++] = add;
        }

        void add(MoreContents more)
        {
            this.moreContents = Arrays.copyOf(moreContents, moreContents.length + 1);
            this.moreContents[moreContents.length - 1] = new MoreContentsHolder(more, length);
        }

        private static <E> E[] resize(E[] array)
        {
            int newLen = array.length == 0 ? 5 : array.length * 2;
            return Arrays.copyOf(array, newLen);
        }

        // reinitialise the moreContents states after a moreContents
        void refill(Stack prefix, MoreContentsHolder holder, int index)
        {
            // drop the transformations that were present when the MoreContents was attached,
            // and prefix any transformations in the new contents (if it's a transformer)
            moreContents = splice(prefix.moreContents, prefix.moreContents.length, moreContents, index, moreContents.length);
            stack = splice(prefix.stack, prefix.length, stack, holder.length, length);
            length += prefix.length - holder.length;
            holder.length = prefix.length;
        }
    }

    private static <E> E[] splice(E[] prefix, int prefixCount, E[] keep, int keepFrom, int keepTo)
    {
        int keepCount = keepTo - keepFrom;
        int newCount = prefixCount + keepCount;
        if (newCount > keep.length)
            keep = Arrays.copyOf(keep, newCount);
        if (keepFrom != prefixCount)
            System.arraycopy(keep, keepFrom, keep, prefixCount, keepCount);
        if (prefixCount != 0)
            System.arraycopy(prefix, 0, keep, 0, prefixCount);
        return keep;
    }
}