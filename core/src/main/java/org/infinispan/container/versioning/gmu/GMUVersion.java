package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class GMUVersion implements IncrementableEntryVersion {

   public static final long NON_EXISTING = -1;

   protected final int viewId;
   protected final String cacheName;
   protected transient ClusterSnapshot clusterSnapshot;
   protected transient int nodeIndex;

   protected GMUVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator) {
      this.cacheName = cacheName;
      this.viewId = viewId;
      clusterSnapshot = versionGenerator.getClusterSnapshot(viewId);
      nodeIndex = clusterSnapshot.indexOf(versionGenerator.getAddress());
      checkState();
   }

   protected GMUVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot, Address localAddress) {
      this.cacheName = cacheName;
      this.viewId = viewId;
      this.clusterSnapshot = clusterSnapshot;
      nodeIndex = clusterSnapshot.indexOf(localAddress);
      checkState();
   }

   public final int getViewId() {
      return viewId;
   }

   public final int getNodeIndex() {
      return this.nodeIndex;
   }
   
   public abstract long getVersionValue(Address address);

   public abstract long getVersionValue(int addressIndex);

   public final long getThisNodeVersionValue() {
      return getVersionValue(nodeIndex);
   }
   
   public int getViewSize() {
      return this.clusterSnapshot.size();
   }

   public static String versionsToString(long[] versions, ClusterSnapshot clusterSnapshot) {
      if (versions == null || versions.length == 0) {
         return "[]";
      } else if (clusterSnapshot != null && versions.length != clusterSnapshot.size()) {
         return "[N/A]";
      }

      if (clusterSnapshot == null) {
         if (versions.length == 1) {
            return "[" + versions[0] + "]";
         } else {
            StringBuilder stringBuilder = new StringBuilder("[");
            stringBuilder.append(versions[0]);
            for (int i = 1; i < versions.length; ++i) {
               stringBuilder.append(",").append(versions[i]);
            }
            return stringBuilder.append("]").toString();
         }
      }
      if (versions.length == 1) {
         return "[" + clusterSnapshot.get(0) + "=" + versions[0] + "]";
      } else {
         StringBuilder stringBuilder = new StringBuilder("[");
         stringBuilder.append(clusterSnapshot.get(0)).append("=").append(versions[0]);

         for (int i = 1; i < versions.length; ++i) {
            stringBuilder.append(",").append(clusterSnapshot.get(i)).append("=").append(versions[i]);
         }
         return stringBuilder.append("]").toString();
      }
   }

   protected final void checkState() {
      if (clusterSnapshot == null) {
         throw new IllegalStateException("Cluster Snapshot in GMU entry version cannot be null");
      } else if (nodeIndex == NON_EXISTING) {
         throw new IllegalStateException("This node index in GMU entry version cannot be null");
      }
   }

   protected final InequalVersionComparisonResult compare(long value1, long value2) {
      if (value1 == NON_EXISTING) {
         return InequalVersionComparisonResult.BEFORE;
      } else if (value2 == NON_EXISTING) {
         return InequalVersionComparisonResult.AFTER;
      }

      int compare = Long.valueOf(value1).compareTo(value2);
      if (compare < 0) {
         return InequalVersionComparisonResult.BEFORE;
      } else if (compare == 0) {
         return InequalVersionComparisonResult.EQUAL;
      }
      return InequalVersionComparisonResult.AFTER;
   }

   protected final Address getAddress() {
      return clusterSnapshot.get(nodeIndex);
   }

   protected static GMUVersionGenerator getGMUVersionGenerator(GlobalComponentRegistry globalComponentRegistry,
                                                               String cacheName) {
      ComponentRegistry componentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      VersionGenerator versionGenerator = componentRegistry.getComponent(VersionGenerator.class);
      return toGMUVersionGenerator(versionGenerator);
   }

   @Override
   public String toString() {
      return "viewId=" + viewId +
            ", nodeIndex=" + nodeIndex +
            ", cacheName=" + cacheName +
            '}';
   }
}
