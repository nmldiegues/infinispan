package org.infinispan.transaction;

import java.io.Serializable;
import java.util.Arrays;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUDistributedVersion;

public class FlagsWrapper implements Serializable {
   
   private static final long serialVersionUID = 678235870477120716L;
   private final boolean hasIncomingEdge;
   private final boolean hasOutgoingEdge;
   private final EntryVersion creationVersion;
   private final long[] computedDepsVersion;
   private final int nodeIndex;
   
   public FlagsWrapper(boolean hasIncomingEdge, boolean hasOutgoingEdge, EntryVersion creationVersion,
         long[] computedDepsVersion, int nodeIndex) {
      super();
      this.hasIncomingEdge = hasIncomingEdge;
      this.hasOutgoingEdge = hasOutgoingEdge;
      this.creationVersion = creationVersion;
      if (computedDepsVersion == null) {
         this.computedDepsVersion = new long[((GMUDistributedVersion)creationVersion).getViewSize()];
         Arrays.fill(this.computedDepsVersion, Long.MAX_VALUE);
      } else {
         this.computedDepsVersion = computedDepsVersion;
      }
      this.nodeIndex = nodeIndex;
   }
   
   public int getNodeIndex() {
      return this.nodeIndex;
   }

   public boolean isHasIncomingEdge() {
      return hasIncomingEdge;
   }

   public boolean isHasOutgoingEdge() {
      return hasOutgoingEdge;
   }

   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   public long[] getComputedDepsVersion() {
      return computedDepsVersion;
   }
   
   public String toString() {
      return "inc: " + hasIncomingEdge + " out: " + hasOutgoingEdge + " creationVersion: " + creationVersion.toString() + " computedDeps: " + Arrays.toString(computedDepsVersion) + " from node: " + nodeIndex;
   }
   
}