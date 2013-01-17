package org.infinispan.transaction;

import java.io.Serializable;

import org.infinispan.container.versioning.EntryVersion;

public class FlagsWrapper implements Serializable {
   
   private static final long serialVersionUID = 678235870477120716L;
   private final boolean hasIncomingEdge;
   private final boolean hasOutgoingEdge;
   private final EntryVersion creationVersion;
   private final long[] computedDepsVersion;
   
   public FlagsWrapper(boolean hasIncomingEdge, boolean hasOutgoingEdge, EntryVersion creationVersion,
         long[] computedDepsVersion) {
      super();
      this.hasIncomingEdge = hasIncomingEdge;
      this.hasOutgoingEdge = hasOutgoingEdge;
      this.creationVersion = creationVersion;
      this.computedDepsVersion = computedDepsVersion;
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
   
}