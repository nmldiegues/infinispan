package org.infinispan.container.entries.gmu;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMURemovedCacheEntry implements InternalGMUCacheEntry {

   private final Object key;
   private final EntryVersion version;

   public InternalGMURemovedCacheEntry(Object key, EntryVersion version) {
      this.key = key;
      this.version = version;
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean isExpired() {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public boolean isNull() {
      return true;
   }

   @Override
   public boolean isChanged() {
      return false;
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public boolean isRemoved() {
      return true;
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public Object getValue() {
      return null;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public void setMaxIdle(long maxIdle) {}

   @Override
   public void setLifespan(long lifespan) {}

   @Override
   public Object setValue(Object value) {
      return null;
   }

   @Override
   public void commit(DataContainer container, EntryVersion newVersion) {}

   @Override
   public void rollback() {}

   @Override
   public void setCreated(boolean created) {}

   @Override
   public void setRemoved(boolean removed) {}

   @Override
   public void setEvicted(boolean evicted) {}

   @Override
   public void setValid(boolean valid) {}

   @Override
   public void setChanged(boolean b) {}

   @Override
   public boolean isLockPlaceholder() {
      return false;
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public void touch() {}

   @Override
   public void touch(long currentTimeMillis) {}

   @Override
   public void reincarnate() {}

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new InternalGMURemovedCacheValue(version);
   }

   @Override
   public InternalCacheEntry clone() {
      try {
         return (InternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("This should never happen");
      }
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {}

   @Override
   public EntryVersion getCreationVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return null;
   }

   @Override
   public boolean isMostRecent() {
      return false;
   }

   @Override
   public InternalCacheEntry getInternalCacheEntry() {
      return null;
   }

   @Override
   public String toString() {
      return "InternalGMURemovedCacheEntry{" +
            "key=" + key +
            ", version=" + version +
            '}';
   }
}
