package org.infinispan.container.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedImmortalCacheEntry;
import org.infinispan.container.gmu.GMUDataContainer.DataContainerVersionBody;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class VersionChain<T> {

   private static final Log log = LogFactory.getLog(VersionChain.class);
   protected VersionBody<T> first;

   public final VersionEntry<T> get(EntryVersion version) {
      VersionBody<T> iterator;
      synchronized (this) {
         iterator = first;
      }

      if (log.isTraceEnabled()) {
         log.tracef("[%s] find value for version %s", Thread.currentThread().getName(), version);
      }

      if (version == null) {
         T entry = iterator == null ? null : iterator.getValue();
         if (log.isTraceEnabled()) {
            log.tracef("[%s] version is null... returning the most recent: %s", Thread.currentThread().getName(), entry);
         }
         return new VersionEntry<T>(entry, null, true);
      }

      EntryVersion nextVersion = null;

      while (iterator != null) {
         if (iterator.isOlderOrEquals(version)) {
            if (log.isTraceEnabled()) {
               log.tracef("[%s] value found: %s", Thread.currentThread().getName(), iterator);
            }
            return new VersionEntry<T>(iterator.getValue(), nextVersion, true);
         }
         nextVersion = iterator.getVersion();
         iterator = iterator.getPrevious();
      }

      if (log.isTraceEnabled()) {
         log.tracef("[%s] No value found!", Thread.currentThread().getName());
      }
      return new VersionEntry<T>(null, nextVersion, false);
   }

   public final VersionBody<T> add(T value, boolean outFlag, long[] creatorVersion) {
      VersionBody<T> toAdd = newValue(value, outFlag, creatorVersion);
      VersionBody<T> iterator = firstAdd(toAdd);
      while (iterator != null) {
         iterator = iterator.add(toAdd);
      }
      return toAdd.getPrevious();
   }

   public final boolean contains(EntryVersion version) {
      VersionBody iterator;
      synchronized (this) {
         iterator = first;
      }

      if (version == null) {
         return iterator != null && !iterator.isRemove();
      }

      while (iterator != null) {
         if (iterator.isOlderOrEquals(version)) {
            return !iterator.isRemove();
         }
         iterator = iterator.getPrevious();
      }
      return false;
   }

   public final VersionEntry<T> remove(T removeObject) {
      // TODO nmld make sure this is not problematic
      VersionBody<T> previous = add(removeObject, false, null);
      T entry = previous == null ? null : previous.getValue();
      //TODO check if is it the most recent
      return new VersionEntry<T>(entry, null, previous != null);
   }

   public final void purgeExpired(long now) {
      VersionBody iterator;
      synchronized (this) {
         while (first != null && first.isExpired(now)) {
            first = first.getPrevious();
         }
         iterator = first;
      }
      while (iterator != null) {
         iterator = iterator.expire(now);
      }
   }

   public void chainToString(StringBuilder stringBuilder) {
      VersionBody iterator;
      synchronized (this) {
         iterator = first;
      }
      while (iterator != null) {
         stringBuilder.append(iterator).append("-->");
         iterator = iterator.getPrevious();
      }
      stringBuilder.append("NULL");
   }

   public final void dumpChain(BufferedWriter writer) throws IOException {
      VersionBody<T> iterator;
      synchronized (this) {
         iterator = first;
      }
      while (iterator != null) {
         writeValue(writer, iterator.getValue());
         Util.safeWrite(writer, "|");
         iterator = iterator.getPrevious();
      }
   }

   public final void gc(EntryVersion minVersion) {
      VersionBody<T> iterator;
      synchronized (this) {
         iterator = first;
      }
      while (iterator != null) {
         iterator = iterator.gc(minVersion);
      }
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder(4096);
         chainToString(stringBuilder);
         log.tracef("Chain after GC: %s", stringBuilder);
      }
   }

   public final int numberOfVersion() {
      VersionBody<T> iterator;
      int size = 0;
      synchronized (this) {
         iterator = first;
      }
      while (iterator != null) {
         size++;
         iterator = iterator.getPrevious();
      }
      return size;
   }

   protected abstract VersionBody<T> newValue(T value, boolean outFlag, long[] creatorVersion);

   protected abstract void writeValue(BufferedWriter writer, T value) throws IOException;

   //return null if the value was added successfully
   private synchronized VersionBody<T> firstAdd(VersionBody<T> body) {
      if (first == null || first.isOlder(body)) {
         body.setPrevious(first);
         first = body;
         return null;
      } else if (first.isEqual(body)) {
         System.out.println("Transaction committing with: " + ((GMUCacheEntryVersion) ((VersionedImmortalCacheEntry)body.getValue()).getVersion()).getThisNodeVersionValue() + " commit time: " + ((GMUCacheEntryVersion) ((VersionedImmortalCacheEntry)body.getValue()).getVersion()).getCreationVersion()[0]);
         first.reincarnate(body);
         return null;
      }
      return first.add(body);
   }
}
