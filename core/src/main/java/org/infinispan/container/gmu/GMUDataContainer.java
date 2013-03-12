package org.infinispan.container.gmu;

import static org.infinispan.container.gmu.GMUEntryFactoryImpl.wrap;
import static org.infinispan.transaction.gmu.GMUHelper.convert;
import static org.infinispan.transaction.gmu.GMUHelper.toInternalGMUCacheEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.container.AbstractDataContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMURemovedCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUDistributedVersion;
import org.infinispan.container.versioning.gmu.GMUReadVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.LockManagerFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUDataContainer extends AbstractDataContainer<GMUDataContainer.DataContainerVersionChain> {

   private static final Log log = LogFactory.getLog(GMUDataContainer.class);
   private CommitLog commitLog;
   private LockManager lockManager;

   protected GMUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
      LockManagerFactory.dataContainer = this;
   }

   protected GMUDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      super(concurrencyLevel, maxEntries, strategy, policy);
      LockManagerFactory.dataContainer = this;
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
                                                    EvictionStrategy strategy, EvictionThreadPolicy policy) {
      return new GMUDataContainer(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new GMUDataContainer(concurrencyLevel);
   }

   public void setLockManager(LockManager lockManager) {
      this.lockManager = lockManager;
   }
   
   @Inject
   public void setCommitLog(CommitLog commitLog) {
      this.commitLog = commitLog;
   }

   @Override
   public InternalCacheEntry get(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, version);
      }
      InternalCacheEntry entry = peek(k, version, false);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, version);
         }

         return new InternalGMUNullCacheEntry(toInternalGMUCacheEntry(entry));
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, version, entry);
      }

      return entry;
   }
   
   public InternalCacheEntry getAsRO(Object k, EntryVersion version, long currentPrepareVersion) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, version);
      }
      InternalCacheEntry entry = peekAsRO(k, version, currentPrepareVersion);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, version);
         }

         return new InternalGMUNullCacheEntry(toInternalGMUCacheEntry(entry));
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, version, entry);
      }

      return entry;
   }
   
   @Override
   public InternalCacheEntry getAsWriteTx(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, version);
      }
      InternalCacheEntry entry = peek(k, version, true);
//      EntryVersion v = entry.getVersion();
//      if (v instanceof GMUCacheEntryVersion) {
//         GMUCacheEntryVersion gmuV = (GMUCacheEntryVersion) v;
//         // System.out.println(Thread.currentThread().getId() + "] read " + k + " with version " + gmuV.getThisNodeVersionValue() + " creation " + gmuV.getCreationVersion()[0] + " and value " + entry.getValue());
//      }
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, version);
         }

         return new InternalGMUNullCacheEntry(toInternalGMUCacheEntry(entry));
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, version, entry);
      }

      return entry;
   }

   public InternalCacheEntry peekAsRO(Object k, EntryVersion version, long currentPrepareVersion) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s)", k, version);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.peek(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, false, version, null, null);
      }

      // System.out.println(Thread.currentThread().getId() + "] marked visible RO read: " + k + " " + ((GMUDistributedVersion)currentVersion).getThisNodeVersionValue());
//      chain.setVisibleRead(currentPrepareVersion);
//      while (lockManager.isLocked(k)) { }
      
      VersionEntry<InternalCacheEntry> entry;
      if (visibleRead) {
         entry = chain.get(getReadVersion(version, false));
      } else {
         entry = chain.get(getReadVersion(version, true));
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s) => %s", k, version, entry);
      }
      EntryVersion creationVersion = entry.getEntry() == null ? null : entry.getEntry().getVersion();

      return wrap(k, entry.getEntry(), entry.isMostRecent(), entry.hasOutgoingEdge(), version, creationVersion, entry.getNextVersion());
   }
   
   @Override
   public InternalCacheEntry peek(Object k, EntryVersion version, boolean writeTx) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s)", k, version);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.peek(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, false, version, null, null);
      }

      VersionEntry<InternalCacheEntry> entry = chain.get(getReadVersion(version, writeTx));
      // System.out.println(Thread.currentThread().getId() + "] Read as WriteTx key " + k + " " + entry + " having max version " + version);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s) => %s", k, version, entry);
      }
      EntryVersion creationVersion = entry.getEntry() == null ? null : entry.getEntry().getVersion();

      return wrap(k, entry.getEntry(), entry.isMostRecent(), entry.hasOutgoingEdge(), version, creationVersion, entry.getNextVersion());
   }

   @Override
   public void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle) {
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null versions!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s)", k, v, version, lifespan, maxIdle);
      }
      GMUCacheEntryVersion cacheEntryVersion = assertGMUCacheEntryVersion(version);
      DataContainerVersionChain chain = entries.get(k);

      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.put(%s,%s,%s,%s,%s), create new VersionChain", k, v, version, lifespan, maxIdle);
         }
         chain = new DataContainerVersionChain();
         entries.put(k, chain);
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s), correct version is %s", k, v, version, lifespan, maxIdle, cacheEntryVersion);
      }

      chain.add(entryFactory.create(k, v, cacheEntryVersion, lifespan, maxIdle), false, null, false);
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder();
         chain.chainToString(stringBuilder);
         log.tracef("Updated chain is %s", stringBuilder);
      }
   }
   
   public void putSSI(Object k, Object v, TxInvocationContext ctx, long lifespan, long maxIdle) {
      EntryVersion version = ctx.getTransactionVersion();
      CacheTransaction cacheTx = ctx.getCacheTransaction();
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null versions!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s)", k, v, version, lifespan, maxIdle);
      }
      GMUCacheEntryVersion cacheEntryVersion = assertGMUCacheEntryVersion(version);
      DataContainerVersionChain chain = entries.get(k);

      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.put(%s,%s,%s,%s,%s), create new VersionChain", k, v, version, lifespan, maxIdle);
         }
         chain = new DataContainerVersionChain();
         entries.put(k, chain);
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s), correct version is %s", k, v, version, lifespan, maxIdle, cacheEntryVersion);
      }

      chain.add(entryFactory.create(k, v, cacheEntryVersion, lifespan, maxIdle), cacheTx.isHasOutgoingEdge(), cacheEntryVersion.getCreationVersion(), cacheEntryVersion.isBoostedVersion());
      chain.addCommit(cacheEntryVersion.getCreationVersion(), cacheTx.isHasOutgoingEdge());
      
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder();
         chain.chainToString(stringBuilder);
         log.tracef("Updated chain is %s", stringBuilder);
      }
   }
   
   public boolean wasReadSince(Object key, GMUDistributedVersion snapshot) {
      DataContainerVersionChain chain = entries.get(key);
      if (chain == null) {
         return false;
      }
//      // System.out.println(Thread.currentThread().getId() + "] validating write: " + key + " starting snapshot " + Arrays.toString(snapshot.getVersions()) + " visible read " + Arrays.toString(chain.visibleReadVersion.get()));
      return chain.wasReadSince(snapshot);
   }

   public void markVisibleRead(Object key, long currentPrepareVersion) {
      DataContainerVersionChain chain = entries.get(key);
      if (chain == null) {
         entries.putIfAbsent(key, new DataContainerVersionChain());
         chain = entries.get(key);
      }
      chain.setVisibleRead(currentPrepareVersion);
   }
   
   @Override
   public boolean containsKey(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s)", k, version);
      }

      VersionChain chain = entries.get(k);
      boolean contains = chain != null && chain.contains(getReadVersion(version, false));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s) => %s", k, version, contains);
      }

      return contains;
   }

   @Override
   public InternalCacheEntry remove(Object k, EntryVersion version) {
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null version!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s)", k, version);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.remove(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, false, null, null, null);
      }
      VersionEntry<InternalCacheEntry> entry = chain.remove(new InternalGMURemovedCacheEntry(k, assertGMUCacheEntryVersion(version)));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s) => %s", k, version, entry);
      }
      return wrap(k, entry.getEntry(), entry.isMostRecent(), entry.hasOutgoingEdge(), null, null, null);
   }

   @Override
   public int size(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s)", version);
      }
      int size = 0;
      for (VersionChain chain : entries.values()) {
         if (chain.contains(getReadVersion(version, false))) {
            size++;
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s) => %s", version, size);
      }
      return size;
   }

   @Override
   public void clear() {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear()");
      }
      entries.clear();
   }

   @Override
   public void clear(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear(%s)", version);
      }
      for (Object key : entries.keySet()) {
         remove(key, version);
      }
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.purgeExpired(%s)", currentTimeMillis);
      }
      for (VersionChain chain : entries.values()) {
         chain.purgeExpired(currentTimeMillis);
      }
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            entry.getValue().dumpChain(bufferedWriter);
            bufferedWriter.newLine();
            bufferedWriter.flush();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   @Override
   public final void gc(EntryVersion minimumVersion) {
      for (DataContainerVersionChain versionChain : entries.values()) {
         versionChain.gc(minimumVersion);
         versionChain.gcCommits(minimumVersion);
      }
   }

   public final VersionChain<?> getVersionChain(Object key) {
      return entries.get(key);
   }

   public final String stateToString() {
      StringBuilder stringBuilder = new StringBuilder(8132);
      for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
         stringBuilder.append(entry.getKey())
               .append("=");
         entry.getValue().chainToString(stringBuilder);
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, DataContainerVersionChain> evicted) {
      Map<Object, InternalCacheEntry> evictedMap = new HashMap<Object, InternalCacheEntry>();
      for (Map.Entry<Object, DataContainerVersionChain> entry : evicted.entrySet()) {
         evictedMap.put(entry.getKey(), entry.getValue().get(null).getEntry());
      }
      return evictedMap;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain evicted) {
      return evicted.get(null).getEntry();
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain entry, EntryVersion version) {
      return entry == null ? null : entry.get(version).getEntry();
   }

   @Override
   protected EntryIterator createEntryIterator(EntryVersion version) {
      return new GMUEntryIterator(version, entries.values().iterator());
   }

   private GMUCacheEntryVersion assertGMUCacheEntryVersion(EntryVersion entryVersion) {
      return convert(entryVersion, GMUCacheEntryVersion.class);
   }

   private GMUReadVersion getReadVersion(EntryVersion entryVersion, boolean isWriteTx) {
      GMUReadVersion gmuReadVersion = commitLog.getReadVersion(entryVersion, isWriteTx);
      if (log.isDebugEnabled()) {
         log.debugf("getReadVersion(%s) ==> %s", entryVersion, gmuReadVersion);
      }
      return gmuReadVersion;
   }
   
   public CommitBody getMostRecentCommit(Object key) {
      return entries.get(key).getMostRecentCommit();
   }

   public static class DataContainerVersionChain extends VersionChain<InternalCacheEntry> {

      // nmld: visible read vector clock, probably an EntryVersion?
      public final AtomicLong visibleRead = new AtomicLong(-1);
      public volatile CommitBody commits;

      protected synchronized void addCommit(long[] commitActualVersion, boolean outgoing) {
         commits = new CommitBody(commitActualVersion, outgoing, commits);
      }

      public void gcCommits(EntryVersion minVersion) {
         CommitBody iterator = this.commits;
         while (iterator != null) {
            CommitBody tmp = iterator.getPrevious();
            if (iterator.isOlderThan(minVersion)) {
               iterator.clearPrevious();
            }
            iterator = tmp;
         }
         
      }
      
      protected boolean wasReadSince(GMUDistributedVersion version) {
         return visibleRead.get() >= version.getThisNodeVersionValue();
      }
      
      protected void setVisibleRead(long currentPrepareVersion) {
         long currentVR = visibleRead.get();
         if (currentPrepareVersion > currentVR) {
            visibleRead.compareAndSet(currentVR, currentPrepareVersion);
         }
      }
      
      protected CommitBody getMostRecentCommit() {
         return this.commits;
      }
      
      @Override
      protected VersionBody<InternalCacheEntry> newValue(InternalCacheEntry value, boolean outFlag, long[] creatorVersion) {
         return new DataContainerVersionBody(value, outFlag, creatorVersion);
      }
      
      @Override
      protected void writeValue(BufferedWriter writer, InternalCacheEntry value) throws IOException {
         writer.write(String.valueOf(value.getValue()));
         writer.write("=");
         writer.write(String.valueOf(value.getVersion()));
      }
   }

   public static class DataContainerVersionBody extends VersionBody<InternalCacheEntry> {

      private final boolean creatorHasOutgoingDep;
      private final long[] creatorActualVersion;
      
      protected DataContainerVersionBody(InternalCacheEntry value, boolean creatorHasOutgoingDep, long[] creatorActualVersion) {
         super(value);
         this.creatorHasOutgoingDep = creatorHasOutgoingDep;
         this.creatorActualVersion = creatorActualVersion;
      }
      
      public boolean hasOutgoingDep() {
         return this.creatorHasOutgoingDep;
      }

      public DataContainerVersionBody getPrevious() {
         return (DataContainerVersionBody) this.previous;
      }
      
      @Override
      public EntryVersion getVersion() {
         return getValue().getVersion();
      }

      @Override
      public boolean isOlder(VersionBody<InternalCacheEntry> otherBody) {
         return isOlder(getValue().getVersion(), otherBody.getVersion());
      }

      @Override
      public boolean isOlderOrEquals(EntryVersion entryVersion) {
         return isOlderOrEquals(getValue().getVersion(), entryVersion);
      }

      @Override
      public boolean isEqual(VersionBody<InternalCacheEntry> otherBody) {
         return isEqual(getValue().getVersion(), otherBody.getVersion());
      }

      @Override
      public boolean isRemove() {
         return getValue().isRemoved();
      }

      @Override
      public void reincarnate(VersionBody<InternalCacheEntry> other) {
//         throw new IllegalStateException("This cannot happen");
         // nmld: in SSI it can happen, and we do nothing
      }

      @Override
      public VersionBody<InternalCacheEntry> gc(EntryVersion minVersion) {
         if (isOlderOrEquals(getValue().getVersion(), minVersion)) {
            VersionBody<InternalCacheEntry> previous = getPrevious();
            //GC previous entries, removing all the references to the previous version entry
            setPrevious(null);
            return previous;
         } else {
            return getPrevious();
         }
      }

      @Override
      protected boolean isExpired(long now) {
         InternalCacheEntry entry = getValue();
         return entry != null && entry.canExpire() && entry.isExpired(now);
      }

      public long[] getCreatorActualVersion() {
         return creatorActualVersion;
      }
      
      @Override
      protected boolean hasOutgoingEdge() {
         return this.creatorHasOutgoingDep;
      }
   }

   private class GMUEntryIterator extends EntryIterator {

      private final EntryVersion version;
      private final Iterator<DataContainerVersionChain> iterator;
      private InternalCacheEntry next;

      private GMUEntryIterator(EntryVersion version, Iterator<DataContainerVersionChain> iterator) {
         this.version = version;
         this.iterator = iterator;
         findNext();
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public InternalCacheEntry next() {
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry toReturn = next;
         findNext();
         return toReturn;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      private void findNext() {
         next = null;
         while (iterator.hasNext()) {
            DataContainerVersionChain chain = iterator.next();
            next = chain.get(version).getEntry();
            if (next != null) {
               return;
            }
         }
      }
   }
}