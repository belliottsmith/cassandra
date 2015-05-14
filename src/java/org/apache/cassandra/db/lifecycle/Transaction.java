/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.lifecycle;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Functions.compose;
import static com.google.common.base.Predicates.*;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singleton;
import static org.apache.cassandra.db.lifecycle.Helpers.*;
import static org.apache.cassandra.db.lifecycle.View.updateCompacting;
import static org.apache.cassandra.db.lifecycle.View.updateLiveSet;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.concurrent.Refs.release;
import static org.apache.cassandra.utils.concurrent.Refs.selfRefs;

public class Transaction extends Transactional.AbstractTransactional
{
    private static final Logger logger = LoggerFactory.getLogger(Tracker.class);

    // a class that represents accumulated modifications to the Tracker;
    // has two instances, one containing modifications that are "staged" (i.e. invisible)
    // and one containing those "logged" that have been made visible through a call to checkpoint()
    private static class State
    {
        // readers that are either brand new, update a previous new reader, or update one of the original readers
        final Set<SSTableReader> update = new HashSet<>();
        // disjoint from update, represents a subset of originals that is no longer needed
        final Set<SSTableReader> obsolete = new HashSet<>();

        void log(State staged)
        {
            update.removeAll(staged.obsolete);
            update.removeAll(staged.update);
            update.addAll(staged.update);
            obsolete.addAll(staged.obsolete);
        }

        boolean contains(SSTableReader reader)
        {
            return update.contains(reader) || obsolete.contains(reader);
        }

        boolean isEmpty()
        {
            return update.isEmpty() && obsolete.isEmpty();
        }

        void clear()
        {
            update.clear();
            obsolete.clear();
        }
    }

    public final Tracker tracker;
    private final OperationType operationType;
    // the original readers this transaction was opened over, and that it guards
    // (no other transactions may operate over these readers concurrently)
    private final Set<SSTableReader> originals = new HashSet<>();
    // the set of readers we've marked as compacting (only updated on creation and in checkpoint())
    private final Set<SSTableReader> marked = new HashSet<>();
    // the identity set of readers we've ever encountered; used to ensure we don't accidentally revisit the
    // same version of a reader. potentially a dangerous property if there are reference counting bugs
    // as they won't be caught until the transaction's lifespan is over.
    // TODO: introduce an inner UniqueIdentifier for SSTableReader instances that can be used safely instead
    private final Set<SSTableReader> identities = Collections.newSetFromMap(new IdentityHashMap<>());

    // changes that have been made visible
    private final State logged = new State();
    // changes that are pending
    private final State staged = new State();

    // construct a Transaction for use in an offline operation
    public static Transaction offline(OperationType operationType, SSTableReader reader)
    {
        return offline(operationType, singleton(reader));
    }

    // construct a Transaction for use in an offline operation
    public static Transaction offline(OperationType operationType, Iterable<SSTableReader> readers)
    {
        // if offline, for simplicity we just use a dummy tracker
        Tracker dummy = new Tracker(null, false);
        dummy.addInitialSSTables(readers);
        dummy.apply(updateCompacting(emptySet(), readers));
        return new Transaction(dummy, operationType, readers);
    }

    Transaction(Tracker tracker, OperationType operationType, Iterable<SSTableReader> readers)
    {
        this.tracker = tracker;
        this.operationType = operationType;
        for (SSTableReader reader : readers)
            originals.add(reader);
        marked.addAll(originals);
        identities.addAll(originals);
    }

    public void doPrepare()
    {
        checkpoint();
    }

    // point of no return: commit all changes, but leave all readers marked as compacting
    public Throwable doCommit(Throwable accumulate)
    {
        // first make our changes visible
        accumulate = checkpoint(accumulate);

        // this is now the point of no return; we cannot safely rollback, so we ignore exceptions until we're done
        // we restore state by obsoleting our obsolete files, releasing our references to them, and updating our size
        // and notification status for the obsolete and new files
        accumulate = setupDeleteNotification(logged.update, tracker, accumulate);
        accumulate = markObsolete(logged.obsolete, accumulate);
        accumulate = tracker.updateSizeTracking(logged.obsolete, logged.update, accumulate);
        accumulate = release(selfRefs(logged.obsolete), accumulate);
        accumulate = tracker.notifySSTablesChanged(originals, logged.update, operationType, accumulate);
        logged.clear();
        return accumulate;
    }

    // undo all of the changes made by this transaction, resetting the state to its original form
    public Throwable doAbort(Throwable accumulate)
    {
        if (logged.isEmpty() && staged.isEmpty())
            return accumulate;

        // mark obsolete all readers that are not versions of those present in the original set
        accumulate = markObsolete(filter_out(concatuniq(staged.update, logged.update),
                                             originals), accumulate);
        // replace all updated readers with a version restored to its original state
        accumulate = tracker.apply(updateLiveSet(logged.update, restoreUpdatedOriginals()), accumulate);
        // setReplaced immediately preceding versions that have not been obsoleted
        accumulate = setReplaced(logged.update, accumulate);
        // we have replaced all of logged.update and never made visible staged.update,
        // and the files we have logged as obsolete we clone fresh versions of, so they are no longer needed either
        // any _staged_ obsoletes should either be in staged.update already, and dealt with there,
        // or is still in its original form (so left as is); in either case no extra action is needed
        accumulate = release(selfRefs(concat(staged.update, logged.update, logged.obsolete)), accumulate);
        logged.clear();
        staged.clear();
        return accumulate;
    }

    protected Throwable doCleanup(Throwable accumulate)
    {
        return unmarkCompacting(marked, accumulate);
    }

    public void permitRedundantTransitions()
    {
        super.permitRedundantTransitions();
    }

    // call when a consistent batch of changes is ready to be made atomically visible
    // these will be exposed in the Tracker atomically, or an exception will be thrown; in this case
    // the transaction should be rolled back
    public void checkpoint()
    {
        maybeFail(checkpoint(null));
    }
    private Throwable checkpoint(Throwable accumulate)
    {
        if (staged.isEmpty())
            return accumulate;

        Set<SSTableReader> toUpdate = toUpdate();
        Set<SSTableReader> fresh = copyOf(fresh());

        // check the current versions of the readers we're replacing haven't somehow been replaced by someone else
        checkNotReplaced(filter_in(toUpdate, staged.update));

        // ensure any new readers are in the compacting set, since we aren't done with them yet
        // and don't want anyone else messing with them
        // apply atomically along with updating the live set of readers
        tracker.apply(compose(updateCompacting(emptySet(), fresh),
                              updateLiveSet(toUpdate, staged.update)));

        // log the staged changes and our newly marked readers
        marked.addAll(fresh);
        logged.log(staged);

        // setup our tracker, and mark our prior versions replaced, also releasing our references to them
        // we do not replace/release obsoleted readers, since we may need to restore them on rollback
        accumulate = setReplaced(filter_out(toUpdate, staged.obsolete), accumulate);
        accumulate = release(selfRefs(filter_out(toUpdate, staged.obsolete)), accumulate);

        staged.clear();
        return accumulate;
    }

    // update a reader: if !original, this is a reader that is being introduced by this transaction;
    // otherwise it must be in the originals() set, i.e. a reader guarded by this transaction
    public void update(SSTableReader reader, boolean original)
    {
        // we add to staged first, so that if our assertions accumulate we can roll it back
        boolean added = staged.update.add(reader)
                     && identities.add(reader);
        assert added;
        // check it isn't obsolete, and that it matches the original flag
        assert !logged.obsolete.contains(reader);
        assert original == originals.contains(reader);
        reader.setupKeyCache();
    }

    // add all of the provided updated readers to the transaction
    public void update(Iterable<SSTableReader> readers, boolean originals)
    {
        for (SSTableReader reader : readers)
            update(reader, originals);
    }

    // mark this reader as for obsoletion. this does not actually obsolete the reader until commit() is called,
    // but on checkpoint() the reader will be removed from the live set
    public void obsolete(SSTableReader reader)
    {
        // check this is: a reader guarded by the transaction, an instance we have already worked with
        // and that we haven't already obsoleted it, nor do we have other changes staged for it
        assert identities.contains(reader);
        assert originals.contains(reader);
        assert !logged.obsolete.contains(reader);
        assert !staged.update.contains(reader);
        assert !staged.obsolete.contains(reader);
        staged.obsolete.add(reader);
    }

    // obsolete every file in the original transaction
    public void obsoleteOriginals()
    {
        // if we're obsoleting, we should have no staged updates for the original files
        assert Iterables.isEmpty(filter_in(staged.update, originals));

        // stage obsoletes for any currently visible versions of any original readers
        Iterables.addAll(staged.obsolete, filter_in(current(), originals));
    }

    // return the readers we're replacing in checkpoint(), i.e. the currently visible version of those in staged
    private Set<SSTableReader> toUpdate()
    {
        return copyOf(filter_in(current(), staged.obsolete, staged.update));
    }

    // new readers that haven't appeared previously (either in the original set or the logged updates)
    private Iterable<SSTableReader> fresh()
    {
        return filter_out(staged.update,
                          originals, logged.update);
    }

    // returns the currently visible readers managed by this transaction
    public Iterable<SSTableReader> current()
    {
        // i.e., those that are updates that have been logged (made visible),
        // and any original readers that have neither been obsoleted nor updated
        return concat(logged.update, filter_out(originals, logged.update, logged.obsolete));
    }

    // transform the provided original readers to
    private List<SSTableReader> restoreUpdatedOriginals()
    {
        Iterable<SSTableReader> torestore = filter_in(originals, logged.update, logged.obsolete);
        return ImmutableList.copyOf(transform(torestore,
                                              new Function<SSTableReader, SSTableReader>()
                                              {
                                                  public SSTableReader apply(SSTableReader reader)
                                                  {
                                                      return current(reader).cloneWithNewStart(reader.first, null);
                                                  }
                                              }));
    }

    // the set of readers guarded by this transaction _in their original instance/state_
    // call current(SSTableReader) on any reader in this set to get the latest instance
    public Set<SSTableReader> originals()
    {
        return Collections.unmodifiableSet(originals);
    }

    // indicates if the reader has been marked for obsoletion
    public boolean isObsolete(SSTableReader reader)
    {
        return logged.obsolete.contains(reader) || staged.obsolete.contains(reader);
    }

    // return the current version of the provided reader, whether or not it is visible or staged;
    // i.e. returns the first version present by testing staged, logged and originals in order.
    public SSTableReader current(SSTableReader reader)
    {
        Set<SSTableReader> container;
        if (staged.contains(reader))
            container = staged.update.contains(reader) ? staged.update : staged.obsolete;
        else if (logged.contains(reader))
            container = logged.update.contains(reader) ? logged.update : logged.obsolete;
        else if (originals.contains(reader))
            container = originals;
        else throw new AssertionError();
        return Iterables.getFirst(filter(container, equalTo(reader)), null);
    }

    // remove the reader from the set we're modifying
    public void cancel(SSTableReader cancel)
    {
        assert originals.contains(cancel);
        assert !staged.contains(cancel);
        assert !logged.contains(cancel);
        originals.remove(cancel);
        marked.remove(cancel);
        maybeFail(unmarkCompacting(singleton(cancel), null));
    }

    // remove the readers from the set we're modifying
    public void cancel(Iterable<SSTableReader> cancels)
    {
        for (SSTableReader cancel : cancels)
            cancel(cancel);
    }

    // remove the provided readers from this Transaction, and return a new Transaction to manage them
    // only permitted to be called if the current Transaction has never been used
    public Transaction split(Collection<SSTableReader> readers)
    {
        checkUnused();
        assert all(readers, in(identities));
        originals.removeAll(readers);
        marked.removeAll(readers);
        identities.removeAll(readers);
        return new Transaction(tracker, operationType, readers);
    }

    // check this transaction has never been used
    private void checkUnused()
    {
        assert logged.isEmpty();
        assert staged.isEmpty();
        assert identities.size() == originals.size();
        assert originals.size() == marked.size();
    }

    private Throwable unmarkCompacting(Set<SSTableReader> unmark, Throwable accumulate)
    {
        accumulate = tracker.apply(updateCompacting(unmark, emptySet()), accumulate);
        // when the CFS is invalidated, it will call unreferenceSSTables().  However, unreferenceSSTables only deals
        // with sstables that aren't currently being compacted.  If there are ongoing compactions that finish or are
        // interrupted after the CFS is invalidated, those sstables need to be unreferenced as well, so we do that here.
        accumulate = tracker.dropSSTablesIfInvalid(accumulate);
        return accumulate;
    }

    // convenience method for callers that know only one sstable is involved in the transaction
    public SSTableReader onlyOne()
    {
        assert originals.size() == 1;
        return Iterables.getFirst(originals, null);
    }

    public String toString()
    {
        return originals.toString();
    }
}