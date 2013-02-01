package org.infinispan.container.gmu;

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

}
