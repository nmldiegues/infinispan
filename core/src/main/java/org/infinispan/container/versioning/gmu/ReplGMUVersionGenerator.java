package org.infinispan.container.versioning.gmu;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheView;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.infinispan.container.versioning.gmu.GMUVersion.NON_EXISTING;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReplGMUVersionGenerator implements GMUVersionGenerator {

   private static final Hash HASH = new MurmurHash3();
   private RpcManager rpcManager;
   private String cacheName;
   private ClusterSnapshot currentClusterSnapshot;
   private int currentViewId;

   public ReplGMUVersionGenerator() {
   }

   @Inject
   public final void init(RpcManager rpcManager, Cache cache) {
      this.rpcManager = rpcManager;
      this.cacheName = cache.getName();
   }

   @Start(priority = 11) // needs to happen *after* the transport starts.
   public final void setEmptyViewId() {
      currentViewId = -1;
      currentClusterSnapshot = new ClusterSnapshot(Collections.singleton(rpcManager.getAddress()), HASH);
   }

   @Override
   public final IncrementableEntryVersion generateNew() {
      return new GMUReplicatedVersion(cacheName, currentViewId, this, 0);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      GMUVersion gmuEntryVersion = toGMUVersion(initialVersion);
      return new GMUReplicatedVersion(cacheName, currentViewId, this, gmuEntryVersion.getThisNodeVersionValue() + 1);
   }

   @Override
   public final GMUVersion mergeAndMax(EntryVersion... entryVersions) {
      //validate the entry versions
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion instanceof GMUReplicatedVersion) {
            continue;
         }
         throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                  entryVersion.getClass().getSimpleName());
      }

      return new GMUReplicatedVersion(cacheName, currentViewId, this, merge(entryVersions));
   }

   @Override
   public final GMUVersion mergeAndMin(EntryVersion... entryVersions) {
      if (entryVersions.length == 0) {
         return new GMUReplicatedVersion(cacheName, currentViewId, this, NON_EXISTING);
      }
      long minVersion = NON_EXISTING;

      for (EntryVersion entryVersion : entryVersions) {
         long value = toGMUVersion(entryVersion).getThisNodeVersionValue();
         if (minVersion == NON_EXISTING) {
            minVersion = value;
         } else if (value != NON_EXISTING) {
            minVersion = Math.min(minVersion, value);
         }
      }

      return new GMUReplicatedVersion(cacheName, currentViewId, this, minVersion);
   }

   @Override
   public final GMUVersion calculateCommitVersion(EntryVersion mergedPrepareVersion,
                                                  Collection<Address> affectedOwners) {
      return updatedVersion(mergedPrepareVersion);
   }

   @Override
   public final GMUCacheEntryVersion convertVersionToWrite(EntryVersion version, int subVersion) {
      GMUVersion gmuVersion = toGMUVersion(version);
      return new GMUCacheEntryVersion(cacheName, currentViewId, this, gmuVersion.getThisNodeVersionValue(), subVersion);
   }

   @Override
   public final GMUReadVersion convertVersionToRead(EntryVersion version, boolean isWriteTx) {
      if (version == null) {
         return null;
      }
      GMUVersion gmuVersion = toGMUVersion(version);
      return new GMUReadVersion(cacheName, currentViewId, this, gmuVersion.getThisNodeVersionValue(), isWriteTx);
   }

   @Override
   public GMUVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                               Collection<Address> alreadyReadFrom) {
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         return null;
      }
      return updatedVersion(transactionVersion);
   }

   @Override
   public GMUVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                               Collection<Address> alreadyReadFrom) {
      return updatedVersion(transactionVersion);
   }

   @Override
   public GMUVersion setNodeVersion(EntryVersion version, long value) {
      return new GMUReplicatedVersion(cacheName, currentViewId, this, value);
   }

   @Override
   public GMUVersion updatedVersion(EntryVersion entryVersion) {
      if (entryVersion instanceof GMUReplicatedVersion) {
         return new GMUReplicatedVersion(cacheName, currentViewId, this,
                                         ((GMUReplicatedVersion) entryVersion).getThisNodeVersionValue());
      } else if (entryVersion instanceof GMUDistributedVersion) {
         int viewId = currentViewId;
         ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
         long[] newVersions = new long[clusterSnapshot.size()];
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            newVersions[i] = ((GMUDistributedVersion) entryVersion).getVersionValue(clusterSnapshot.get(i));
         }
         return new GMUDistributedVersion(cacheName, viewId, this, newVersions);
      }
      throw new IllegalArgumentException("Cannot handle " + entryVersion);
   }

   @Override
   public synchronized final ClusterSnapshot getClusterSnapshot(int viewId) {
      return currentClusterSnapshot;
   }

   @Override
   public final Address getAddress() {
      return rpcManager.getAddress();
   }

   @Override
   public void updateViewHistory(List<CacheView> viewHistory) {
      for (CacheView cacheView : viewHistory) {
         addCacheView(cacheView);
      }
   }

   @Override
   public synchronized void addCacheView(CacheView cacheView) {
      if (currentViewId >= cacheView.getViewId()) {
         return;
      }
      currentViewId = cacheView.getViewId();
      currentClusterSnapshot = new ClusterSnapshot(cacheView.getMembers(), HASH);
      notifyAll();
   }

   @Override
   public void gcCacheView(int minViewId) {
      //no-op
   }

   @Override
   public int getViewHistorySize() {
      return 1; //only the most recent in saved
   }

   private long merge(EntryVersion... entryVersions) {
      long max = NON_EXISTING;
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion == null) {
            continue;
         }
         GMUVersion gmuVersion = toGMUVersion(entryVersion);
         max = Math.max(max, gmuVersion.getThisNodeVersionValue());
      }
      return max;
   }
}
