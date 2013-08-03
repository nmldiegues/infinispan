/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.transaction.gmu.manager;

import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SortedTransactionQueue {

   private static final Log log = LogFactory.getLog(SortedTransactionQueue.class);
   private final ConcurrentHashMap<GlobalTransaction, Node> concurrentHashMap;
   private final Node firstEntry;
   private final Node lastEntry;

   public SortedTransactionQueue() {
      this.concurrentHashMap = new ConcurrentHashMap<GlobalTransaction, Node>();

      this.firstEntry = new AbstractBoundaryNode() {
         private Node first;

         @Override
         public Node getNext() {
            return first;
         }

         @Override
         public void setNext(Node next) {
            this.first = next;
         }

         @Override
         public int compareTo(Node o) {
            //the first node is always lower
            return -1;
         }

         @Override
         public String toString() {
            return "FIRST_ENTRY";
         }
      };

      this.lastEntry = new AbstractBoundaryNode() {
         private Node last;

         @Override
         public Node getPrevious() {
            return last;
         }

         @Override
         public void setPrevious(Node previous) {
            this.last = previous;
         }

         @Override
         public int compareTo(Node o) {
            //the last node is always higher
            return 1;
         }

         @Override
         public String toString() {
            return "LAST_ENTRY";
         }
      };

      firstEntry.setNext(lastEntry);
      lastEntry.setPrevious(firstEntry);
   }

   public final void prepare(CacheTransaction cacheTransaction, long concurrentClockNumber) {
      GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
      if (concurrentHashMap.contains(globalTransaction)) {
         log.warnf("Duplicated prepare for %s", globalTransaction);
      }
      Node entry = new TransactionEntryImpl(cacheTransaction, concurrentClockNumber);
      concurrentHashMap.put(globalTransaction, entry);
      addNew(entry);
      hasTransactionReadyToCommit();
   }

   public final void rollback(CacheTransaction cacheTransaction) {
      remove(concurrentHashMap.remove(cacheTransaction.getGlobalTransaction()));
      hasTransactionReadyToCommit();
   }

   //return true if it is a read-write transaction
   public final TransactionEntry commit(GlobalTransaction globalTransaction, GMUVersion commitVersion) {
      Node entry = concurrentHashMap.get(globalTransaction);
      if (entry == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Cannot commit transaction %s. Maybe it is a read-only on this node",
                       globalTransaction.globalId());
         }
         return entry;
      }
      update(entry, commitVersion);
      hasTransactionReadyToCommit();
      return entry;
   }

   public final synchronized void populateToCommit(List<TransactionEntry> transactionEntryList) {
      removeCommitted();
      if (!firstEntry.getNext().hasReceiveCommitCommand()) {
         if (log.isTraceEnabled()) {
            log.tracef("get transactions to commit. First is not ready! %s", firstEntry.getNext());
         }
         return;
      }

      //if (log.isDebugEnabled()) {
      //   log.debugf("Try to commit transaction. Queue is %s", queueToString());
      //}

      Node firstTransaction = firstEntry.getNext();

      Node transactionToCheck = firstTransaction.getNext();

      while (transactionToCheck != lastEntry) {
         boolean isSameVersion = transactionToCheck.compareTo(firstTransaction) == 0;
         if (!isSameVersion) {
            //commit until this transaction
            commitUntil(transactionToCheck, transactionEntryList);
            return;
         } else if (!transactionToCheck.hasReceiveCommitCommand()) {
            if (log.isTraceEnabled()) {
               log.tracef("Transaction with the same version not ready. %s and %s", firstTransaction, transactionToCheck);
            }
            return;
         }
         transactionToCheck = transactionToCheck.getNext();
      }
      //commit until this transaction
      commitUntil(transactionToCheck, transactionEntryList);
   }

   /**
    * @return {@code true} if it has already transactions to be committed
    */
   public final synchronized boolean hasTransactionReadyToCommit() {
      removeCommitted();
      //same code as populateToCommit...
      if (!firstEntry.getNext().hasReceiveCommitCommand()) {
         if (log.isTraceEnabled()) {
            log.tracef("hasTransactionReadyToCommit() == false. %s", firstEntry.getNext());
         }
         return false;
      }
      firstEntry.getNext().notifyFirstInQueue();

      Node firstTransaction = firstEntry.getNext();

      Node transactionToCheck = firstTransaction.getNext();

      while (transactionToCheck != lastEntry) {
         boolean isSameVersion = transactionToCheck.compareTo(firstTransaction) == 0;
         if (!isSameVersion) {
            if (log.isTraceEnabled()) {
               log.tracef("hasTransactionReadyToCommit() == true. %s", firstTransaction);
            }
            transactionToCheck = transactionToCheck.getPrevious();
            while (transactionToCheck != firstEntry) {
               transactionToCheck.markReadyToCommit();
               if (log.isTraceEnabled()) {
                  log.tracef("Mark ready to commit: %s", transactionToCheck);
               }
               transactionToCheck = transactionToCheck.getPrevious();
            }
            return true;
         } else if (!transactionToCheck.hasReceiveCommitCommand()) {
            if (log.isTraceEnabled()) {
               log.tracef("hasTransactionReadyToCommit() == false. %s is waiting for %s", firstTransaction, transactionToCheck);
            }
            return false;
         }
         transactionToCheck = transactionToCheck.getNext();
      }
      if (log.isTraceEnabled()) {
         log.tracef("hasTransactionReadyToCommit() == true. %s", firstTransaction);
      }
      transactionToCheck = transactionToCheck.getPrevious();
      while (transactionToCheck != firstEntry) {
         transactionToCheck.markReadyToCommit();
         if (log.isTraceEnabled()) {
            log.tracef("Mark ready to commit: %s", transactionToCheck);
         }
         transactionToCheck = transactionToCheck.getPrevious();
      }
      return true;
   }

   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return concurrentHashMap.get(globalTransaction);
   }

   public final synchronized String queueToString() {
      Node node = firstEntry.getNext();
      if (node == lastEntry) {
         return "[]";
      }
      StringBuilder builder = new StringBuilder("[");
      builder.append(node);

      node = node.getNext();
      while (node != lastEntry) {
         builder.append(",").append(node);
         node.getNext();
      }
      builder.append("]");
      return builder.toString();
   }

   public final int size() {
      Node node = firstEntry.getNext();
      int count = 0;
      while (node != lastEntry) {
         count++;
         node = node.getNext();
      }
      return count;
   }

   private void checkWaitingTime(Node entry) {
      Node iterator = entry.getPrevious();
      boolean waiting = iterator != firstEntry;
      boolean pendingFound = false;
      while (iterator != firstEntry) {
         if (!iterator.hasReceiveCommitCommand()) {
            pendingFound = true;
            break;
         }
         iterator = iterator.getPrevious();
      }
      entry.setWaitingType(pendingFound);
      entry.setWaiting(waiting);
      firstEntry.getNext().notifyFirstInQueue();
   }

   private void commitUntil(Node exclusive, List<TransactionEntry> transactionEntryList) {
      Node transaction = firstEntry.getNext();

      while (transaction != exclusive) {
         transactionEntryList.add(transaction);
         transaction = transaction.getNext();
      }
   }

   private void removeCommitted() {
      Node node = firstEntry.getNext();
      while (node != lastEntry) {
         if (node.isCommitted()) {
            node.markReadyToCommit(); //to be sure that is unblocked
            node = node.getNext();
         } else {
            break;
         }
      }
      Node newFirst = node;
      firstEntry.setNext(newFirst);
      newFirst.setPrevious(firstEntry);
   }

   private synchronized void update(Node entry, GMUVersion commitVersion) {
      if (log.isTraceEnabled()) {
         log.tracef("Update %s with %s", entry, commitVersion);
      }

      entry.commitVersion(commitVersion);
      if (entry.compareTo(entry.getNext()) > 0) {
         Node insertBefore = entry.getNext().getNext();
         remove(entry);
         while (entry.compareTo(insertBefore) > 0) {
            insertBefore = insertBefore.getNext();
         }
         addBefore(insertBefore, entry);
      }
      if (TransactionsStatisticsRegistry.isGmuWaitingActive()) {
         checkWaitingTime(entry);
      }
   }

   private synchronized void addNew(Node entry) {
      if (log.isTraceEnabled()) {
         log.tracef("Add new entry: %s", entry);
      }
      Node insertAfter = lastEntry.getPrevious();

      while (insertAfter != firstEntry) {
         if (insertAfter.compareTo(entry) <= 0) {
            break;
         }
         insertAfter = insertAfter.getPrevious();
      }
      addAfter(insertAfter, entry);
      if (log.isTraceEnabled()) {
         log.tracef("After add, first entry is %s", firstEntry.getNext());
      }
   }

   private synchronized void remove(Node entry) {
      if (entry == null) {
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("remove entry: %s", entry);
      }

      Node previous = entry.getPrevious();
      Node next = entry.getNext();
      entry.setPrevious(null);
      entry.setNext(null);

      previous.setNext(next);
      next.setPrevious(previous);
      if (log.isTraceEnabled()) {
         log.tracef("After remove, first entry is %s", firstEntry.getNext());
      }
   }

   private synchronized void addAfter(Node insertAfter, Node entry) {
      entry.setNext(insertAfter.getNext());
      insertAfter.getNext().setPrevious(entry);

      entry.setPrevious(insertAfter);
      insertAfter.setNext(entry);

      if (log.isTraceEnabled()) {
         log.tracef("add after: %s --> [%s] --> %s", insertAfter, entry, entry.getNext());
      }
   }

   private void addBefore(Node insertBefore, Node entry) {
      entry.setPrevious(insertBefore.getPrevious());
      insertBefore.getPrevious().setNext(entry);

      entry.setNext(insertBefore);
      insertBefore.setPrevious(entry);
      if (log.isTraceEnabled()) {
         log.tracef("add before: %s --> [%s] --> %s", entry.getPrevious(), entry, insertBefore);
      }
   }

   private static enum TxState {
      COMMIT_RECEIVED(1), //1 << 0
      READY_TO_COMMIT(1 << 1),
      COMMITTING(1 << 2),
      COMMITTED(1 << 3),
      WAITED(1 << 4),
      PENDING_TX_BEFORE(1 << 5),;
      private final byte mask;

      private TxState(int mask) {
         this.mask = (byte) mask;
      }

      public final byte set(byte state) {
         return (byte) (state | mask);
      }

      public final boolean isSet(byte state) {
         return (state & mask) != 0;
      }

      public static final String stateToString(byte state) {
         if (state == 0) {
            return "[]";
         }
         StringBuilder builder = new StringBuilder();
         for (TxState txState : values()) {
            if (txState.isSet(state)) {
               builder.append(",").append(txState);
            }
         }
         builder.replace(0, 1, "[");
         builder.append("]");
         return builder.toString();
      }
   }

   private interface Node extends TransactionEntry, Comparable<Node> {
      /**
       * adds the commit entry
       *
       * @param commitCommand commit command
       */
      void commitVersion(GMUVersion commitCommand);

      /**
       * @return the current version
       */
      GMUVersion getVersion();

      /**
       * @return {@code true} if the transaction has already received the commit command
       */
      boolean hasReceiveCommitCommand();

      /**
       * marks the transaction ready to commit. This means that {@link #hasReceiveCommitCommand()} is true and it is the
       * first entry in the queue
       */
      void markReadyToCommit();

      Node getPrevious();

      void setPrevious(Node previous);

      Node getNext();

      void setNext(Node next);

      void notifyFirstInQueue();

      void setWaitingType(boolean pendingFound);

      void setWaiting(boolean waiting);
   }

   public static interface TransactionEntry {
      /**
       * waits until this entry if the first in the queue
       *
       * @throws InterruptedException
       */
      void awaitUntilIsReadyToCommit() throws InterruptedException;

      /**
       * @return {@code true} if this transaction has receive the commit command and it is the first in the queue
       */
      boolean isReadyToCommit();

      /**
       * @return {@link CacheTransaction} with the version updated
       */
      CacheTransaction getCacheTransactionForCommit();

      /**
       * marks this entry has committed, i.e. the modifications have been flushed in the data container
       */
      void committed();

      /**
       * @return {@code true} if this entry was committed
       */
      boolean isCommitted();

      GlobalTransaction getGlobalTransaction();

      void awaitUntilCommitted() throws InterruptedException;

      long getConcurrentClockNumber();

      long getCommitReceivedTimestamp();

      boolean isWaitBecauseOfPendingTx();

      long getReadyToCommitTimestamp();

      long getFirstInQueueTimestamp();

      boolean hasWaited();

      boolean committing();
   }

   private class TransactionEntryImpl implements Node {

      private final CacheTransaction cacheTransaction;
      private final CountDownLatch readyToCommit;
      private volatile byte state;
      private volatile GMUVersion entryVersion;
      private long concurrentClockNumber; //This is the value of the last committed vector clock's n-th entry on this node n at the time this object was created.
      private volatile long commitReceivedTimestamp = -1;
      private volatile long firstInQueueTimestamp = -1;
      private volatile long readyToCommitTimestamp = -1;
      private Node previous;
      private Node next;

      private TransactionEntryImpl(CacheTransaction cacheTransaction, long concurrentClockNumber) {
         this.state = 0;
         this.cacheTransaction = cacheTransaction;
         this.entryVersion = toGMUVersion(cacheTransaction.getTransactionVersion());
         this.readyToCommit = new CountDownLatch(1);
         this.concurrentClockNumber = concurrentClockNumber;
      }

      public synchronized void commitVersion(GMUVersion commitVersion) {
         this.entryVersion = commitVersion;
         this.state = TxState.COMMIT_RECEIVED.set(state);
         if (log.isTraceEnabled()) {
            log.tracef("Set transaction commit version: %s", this);
         }
         if (TransactionsStatisticsRegistry.isActive()) {
            commitReceivedTimestamp = System.nanoTime();
         }
      }

      @Override
      public synchronized void setWaitingType(boolean pendingFound) {
         if (pendingFound) {
            this.state = TxState.PENDING_TX_BEFORE.set(state);
         }
      }

      @Override
      public void notifyFirstInQueue() {
         if (firstInQueueTimestamp == -1 && TransactionsStatisticsRegistry.isActive()) {
            firstInQueueTimestamp = System.nanoTime();
         }
      }

      @Override
      public synchronized void setWaiting(boolean waiting) {
         if (waiting) {
            this.state = TxState.WAITED.set(state);
         }
      }

      public synchronized GMUVersion getVersion() {
         return entryVersion;
      }

      public synchronized long getConcurrentClockNumber() {
         return concurrentClockNumber;
      }

      public CacheTransaction getCacheTransaction() {
         return cacheTransaction;
      }

      public synchronized boolean hasReceiveCommitCommand() {
         return TxState.COMMIT_RECEIVED.isSet(state);
      }

      @Override
      public synchronized void markReadyToCommit() {
         this.state = TxState.READY_TO_COMMIT.set(state);
         readyToCommit.countDown();
         readyToCommitTimestamp = System.nanoTime();
      }

      @Override
      public synchronized boolean isCommitted() {
         return TxState.COMMITTED.isSet(state);
      }

      public GlobalTransaction getGlobalTransaction() {
         return cacheTransaction.getGlobalTransaction();
      }

      @Override
      public synchronized void awaitUntilCommitted() throws InterruptedException {
         while (!isCommitted()) {
            wait();
         }
      }

      @Override
      public synchronized boolean committing() {
         if (TxState.COMMITTING.isSet(state)) {
            return false;
         }
         this.state = TxState.COMMITTING.set(state);
         return true;
      }

      public synchronized void committed() {
         if (log.isTraceEnabled()) {
            log.tracef("Mark transaction committed: %s", this);
         }
         this.state = TxState.COMMITTED.set(state);
         notifyAll();
      }

      @Override
      public void awaitUntilIsReadyToCommit() throws InterruptedException {
         readyToCommit.await();
      }

      @Override
      public synchronized boolean isReadyToCommit() {
         return TxState.READY_TO_COMMIT.isSet(state);
      }

      @Override
      public CacheTransaction getCacheTransactionForCommit() {
         cacheTransaction.setTransactionVersion(entryVersion);
         return cacheTransaction;
      }

      @Override
      public String toString() {
         return "TransactionEntry{" +
               "version=" + getVersion() +
               ", state=" + TxState.stateToString(state) +
               ", gtx=" + cacheTransaction.getGlobalTransaction().globalId() +
               '}';
      }

      @Override
      public int compareTo(Node otherNode) {
         if (otherNode == null) {
            return -1;
         } else if (otherNode == firstEntry) {
            return 1;
         } else if (otherNode == lastEntry) {
            return -1;
         }

         Long my = getVersion().getThisNodeVersionValue();
         Long other = otherNode.getVersion().getThisNodeVersionValue();
         int compareResult = my.compareTo(other);

         if (log.isTraceEnabled()) {
            log.tracef("Comparing this[%s] with other[%s]. compare(%s,%s) ==> %s", this, otherNode, my, other,
                       compareResult);
         }

         return compareResult;
      }

      @Override
      public Node getPrevious() {
         return previous;
      }

      @Override
      public void setPrevious(Node previous) {
         this.previous = previous;
      }

      @Override
      public Node getNext() {
         return next;
      }

      @Override
      public void setNext(Node next) {
         this.next = next;
      }

      @Override
      public final long getCommitReceivedTimestamp() {
         return commitReceivedTimestamp;
      }

      @Override
      public final synchronized boolean isWaitBecauseOfPendingTx() {
         return TxState.PENDING_TX_BEFORE.isSet(state);
      }

      @Override
      public final long getReadyToCommitTimestamp() {
         return readyToCommitTimestamp;
      }

      @Override
      public final long getFirstInQueueTimestamp() {
         return firstInQueueTimestamp;
      }

      @Override
      public final boolean hasWaited() {
         return TxState.WAITED.isSet(state);
      }
   }

   private abstract class AbstractBoundaryNode implements Node {

      @Override
      public void commitVersion(GMUVersion commitCommand) {/*no-op*/}

      @Override
      public GMUVersion getVersion() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean committing() {
         return false;
      }

      @Override
      public boolean hasReceiveCommitCommand() {
         return false;
      }

      @Override
      public void markReadyToCommit() {/*no-op*/}

      @Override
      public GlobalTransaction getGlobalTransaction() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Node getPrevious() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setPrevious(Node previous) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Node getNext() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setNext(Node next) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void awaitUntilIsReadyToCommit() throws InterruptedException {/*no-op*/}

      @Override
      public boolean isReadyToCommit() {
         throw new UnsupportedOperationException();
      }

      @Override
      public CacheTransaction getCacheTransactionForCommit() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void committed() {/*no-op*/}

      @Override
      public boolean isCommitted() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void awaitUntilCommitted() throws InterruptedException {/*no-op*/}

      @Override
      public long getConcurrentClockNumber() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void notifyFirstInQueue() {
         //no-op, stats methods
      }

      @Override
      public void setWaitingType(boolean pendingFound) {
         //no-op, stats methods
      }

      @Override
      public long getCommitReceivedTimestamp() {
         return -1;
      }

      @Override
      public boolean isWaitBecauseOfPendingTx() {
         return false;
      }

      @Override
      public long getReadyToCommitTimestamp() {
         return -1;
      }

      @Override
      public long getFirstInQueueTimestamp() {
         return -1;
      }

      @Override
      public void setWaiting(boolean waiting) {
         //no-op
      }

      @Override
      public boolean hasWaited() {
         return false;
      }
   }
}
