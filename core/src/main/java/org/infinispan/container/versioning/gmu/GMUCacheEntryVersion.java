package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUCacheEntryVersion extends GMUVersion {

   private final long version;
   private final int subVersion;
   // nmld: may differ from version
   private final long[] creationVersion;

   public GMUCacheEntryVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version,
                               int subVersion, long[] creationVersion) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
      if (this.version == Long.MAX_VALUE) {
         System.out.println("Here " + version + " " + Arrays.toString(creationVersion));
      }
      this.subVersion = subVersion;
      this.creationVersion = creationVersion;
   }

   private GMUCacheEntryVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot, Address localAddress,
                                long version, int subVersion, long[] creationVersion) {
      super(cacheName, viewId, clusterSnapshot, localAddress);
      this.version = version;
      if (this.version == Long.MAX_VALUE) {
         System.out.println("Here private " + version + " " + Arrays.toString(creationVersion));
      }
      this.subVersion = subVersion;
      this.creationVersion = creationVersion;
   }

   @Override
   public final long getVersionValue(Address address) {
      return getVersionValue(clusterSnapshot.indexOf(address));
   }

   @Override
   public final long getVersionValue(int addressIndex) {
      if (addressIndex == nodeIndex) {
         return version;
      }
      return NON_EXISTING;
   }

   public final int getSubVersion() {
      return subVersion;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      //this particular version can only be compared with this type of GMU version or with GMUReadVersion
      if (other == null) {
         return BEFORE;
      } else if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         InequalVersionComparisonResult result = compare(version, cacheEntryVersion.version);
         if (result == EQUAL) {
            return compare(subVersion, cacheEntryVersion.subVersion);
         }
         return result;
      } else if (other instanceof GMUReadVersion) {
         GMUReadVersion readVersion = (GMUReadVersion) other;
         // TODO: nmld understand the following
         if (readVersion.contains(version, subVersion)) {
            //this is an invalid version. set it higher
            return AFTER;
         }
         if (readVersion.isReadFromWriteTx()) {
            return compare(creationVersion[nodeIndex], readVersion.getThisNodeVersionValue());
         } else {
            return compare(version, readVersion.getThisNodeVersionValue());
         }
      } else if (other instanceof GMUReplicatedVersion) {
         GMUReplicatedVersion replicatedVersion = (GMUReplicatedVersion) other;
         InequalVersionComparisonResult result = compare(version, replicatedVersion.getThisNodeVersionValue());
         if (result == EQUAL) {
            return compare(viewId, replicatedVersion.getViewId());
         }
         return result;
      }  else if (other instanceof GMUDistributedVersion) {
         GMUDistributedVersion distributedVersion = (GMUDistributedVersion) other;
         InequalVersionComparisonResult result = compare(creationVersion[nodeIndex], distributedVersion.getThisNodeVersionValue());
         if (result == EQUAL) {
            return compare(viewId, distributedVersion.getViewId());
         }
         return result;
      }
      throw new IllegalArgumentException("Cannot compare " + getClass() + " with " + other.getClass());
   }

   @Override
   public String toString() {
      return "GMUCacheEntryVersion{" +
            "version=" + version +
            ", subVersion=" + subVersion +
            ", " + super.toString();
   }

   public static class Externalizer extends AbstractExternalizer<GMUCacheEntryVersion> {

      private final GlobalComponentRegistry globalComponentRegistry;

      public Externalizer(GlobalComponentRegistry globalComponentRegistry) {
         this.globalComponentRegistry = globalComponentRegistry;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends GMUCacheEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends GMUCacheEntryVersion>>asSet(GMUCacheEntryVersion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, GMUCacheEntryVersion object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeInt(object.viewId);
         output.writeLong(object.version);
         output.writeInt(object.subVersion);
         output.writeObject(object.creationVersion);
      }

      @Override
      public GMUCacheEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         GMUVersionGenerator gmuVersionGenerator = getGMUVersionGenerator(globalComponentRegistry, cacheName);
         int viewId = input.readInt();
         ClusterSnapshot clusterSnapshot = gmuVersionGenerator.getClusterSnapshot(viewId);
         if (clusterSnapshot == null) {
            throw new IllegalArgumentException("View Id " + viewId + " not found in this node");
         }
         long version = input.readLong();
         int subVersion = input.readInt();
         long[] creation = (long[]) input.readObject();
         return new GMUCacheEntryVersion(cacheName, viewId, clusterSnapshot, gmuVersionGenerator.getAddress(), version,
                                         subVersion, creation);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_CACHE_VERSION;
      }
   }

   public long[] getCreationVersion() {
      return creationVersion;
   }
}
