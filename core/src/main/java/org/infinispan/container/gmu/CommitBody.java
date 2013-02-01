package org.infinispan.container.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUDistributedVersion;

public class CommitBody {
   
   private final long[] creatorActualVersion;
   private final boolean outgoing;
   private volatile CommitBody previous;
   
   public CommitBody(long[] creatorActualVersion, boolean outgoing, CommitBody previous) {
      this.creatorActualVersion = creatorActualVersion;
      this.outgoing = outgoing;
      this.previous = previous;
   }

   public long[] getCreatorActualVersion() {
      return creatorActualVersion;
   }

   public boolean isOutgoing() {
      return outgoing;
   }

   public CommitBody getPrevious() {
      return previous;
   }
   
   public void clearPrevious() {
      this.previous = null;
   }

   public boolean isMoreRecentThan(EntryVersion version) {
      if (version == null) {
         return false;
      }
      GMUDistributedVersion distVersion = (GMUDistributedVersion) version;
      return creatorActualVersion[distVersion.getNodeIndex()] > distVersion.getThisNodeVersionValue();
   }
   
   public boolean isOlderThan(EntryVersion version) {
      if (version == null) {
         return true;
      }
      GMUDistributedVersion distVersion = (GMUDistributedVersion) version;
      return creatorActualVersion[distVersion.getNodeIndex()] < distVersion.getThisNodeVersionValue();
   }
}