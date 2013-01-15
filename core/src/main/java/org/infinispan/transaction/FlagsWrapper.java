package org.infinispan.transaction;

import java.io.Serializable;

import org.infinispan.container.versioning.EntryVersion;

public class FlagsWrapper implements Serializable {
   
   private static final long serialVersionUID = 678235870477120716L;
   private final boolean hasIncomingEdge;
   private final boolean hasOutgoingEdge;
   private final long[] creationVersion;
   private final EntryVersion preparedVersion;
   
   public FlagsWrapper(boolean hasIncomingEdge, boolean hasOutgoingEdge, long[] creationVersion,
         EntryVersion preparedVersion) {
      super();
      this.hasIncomingEdge = hasIncomingEdge;
      this.hasOutgoingEdge = hasOutgoingEdge;
      this.creationVersion = creationVersion;
      this.preparedVersion = preparedVersion;
   }

   public boolean isHasIncomingEdge() {
      return hasIncomingEdge;
   }

   public boolean isHasOutgoingEdge() {
      return hasOutgoingEdge;
   }

   public long[] getCreationVersion() {
      return creationVersion;
   }

   public EntryVersion getPreparedVersion() {
      return preparedVersion;
   }
   
}