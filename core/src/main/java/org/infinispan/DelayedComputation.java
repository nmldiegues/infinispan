package org.infinispan;

import java.io.Serializable;
import java.util.Collection;


public abstract class DelayedComputation<T> implements Serializable {

   public abstract Collection<Object> getAffectedKeys();
   
   public abstract T compute();
   
   @Override
   public boolean equals(Object obj) {
      if (! (obj instanceof DelayedComputation)) {
         return false;
      }
      DelayedComputation other = (DelayedComputation) obj;
      return this.getAffectedKeys().iterator().next().equals(other.getAffectedKeys().iterator().next());
   }
   
   @Override
   public int hashCode() {
      return getAffectedKeys().iterator().next().hashCode();
   }
   
   public abstract void mergeNewDelayedComputation(DelayedComputation newComputation);
}
