package org.infinispan;

import java.util.HashSet;
import java.util.Set;

public class DelayedActionsHolder {
   
   public Set<DelayedComputation> computations;
   public long timestamp;
   
   public DelayedActionsHolder() {
      this.computations = new HashSet<DelayedComputation>();
      this.timestamp = System.currentTimeMillis();
   }
   
}