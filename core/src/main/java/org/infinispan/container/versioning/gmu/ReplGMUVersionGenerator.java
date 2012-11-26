package org.infinispan.container.versioning.gmu;

import org.infinispan.Cache;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.infinispan.container.versioning.gmu.GMUEntryVersion.NON_EXISTING;

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

   public ReplGMUVersionGenerator() {}

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
      return new GMUCacheEntryVersion(cacheName, currentViewId, this, 0);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      GMUCacheEntryVersion gmuEntryVersion = toGMUEntryVersion(initialVersion);
      return new GMUCacheEntryVersion(cacheName, currentViewId, this, gmuEntryVersion.getThisNodeVersionValue() + 1);
   }

   @Override
   public final GMUEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions) {
      //validate the entry versions
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion instanceof GMUCacheEntryVersion) {
            continue;
         }
         throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                  entryVersion.getClass().getSimpleName());
      }

      return new GMUCacheEntryVersion(cacheName, currentViewId, this, merge(entryVersions));
   }

   @Override
   public final GMUEntryVersion calculateCommitVersion(EntryVersion mergedPrepareVersion,
                                                       Collection<Address> affectedOwners) {
      return toGMUEntryVersion(mergedPrepareVersion);
   }

   @Override
   public final GMUEntryVersion convertVersionToWrite(EntryVersion version) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);
      return new GMUCacheEntryVersion(cacheName, currentViewId, this, gmuEntryVersion.getThisNodeVersionValue());
   }

   @Override
   public GMUEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                                    Collection<Address> alreadyReadFrom) {
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         return null;
      }
      return toGMUEntryVersion(transactionVersion);
   }

   @Override
   public GMUEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                                    Collection<Address> alreadyReadFrom) {
      return toGMUEntryVersion(transactionVersion);
   }

   @Override
   public GMUEntryVersion setNodeVersion(EntryVersion version, long value) {
      return new GMUCacheEntryVersion(cacheName, currentViewId, this, value);
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
   public void updateCacheTopologyHistory(List<CacheTopology> viewHistory) {
      for (CacheTopology cacheTopology : viewHistory) {
         updateCacheTopology(cacheTopology);
      }
   }

   @Override
   public synchronized void updateCacheTopology(CacheTopology cacheTopology) {
      if (currentViewId >= cacheTopology.getTopologyId()) {
         return;
      }
      currentViewId = cacheTopology.getTopologyId();
      currentClusterSnapshot = new ClusterSnapshot(cacheTopology.getMembers(), HASH);
      notifyAll();
   }

   private GMUCacheEntryVersion toGMUEntryVersion(EntryVersion version) {
      if (version instanceof GMUCacheEntryVersion) {
         return (GMUCacheEntryVersion) version;
      }
      throw new IllegalArgumentException("Expected a GMU entry version but received " + version.getClass().getSimpleName());
   }

   private long merge(Collection<? extends EntryVersion> entryVersions) {
      long max = NON_EXISTING;
      for (EntryVersion entryVersion : entryVersions) {
         GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(entryVersion);
         max = Math.max(max, gmuEntryVersion.getThisNodeVersionValue());
      }
      return max;
   }
}
