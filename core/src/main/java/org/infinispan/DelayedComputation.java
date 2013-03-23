package org.infinispan;

import java.util.Collection;


public interface DelayedComputation<T> {

   public abstract Collection<Object> getAffectedKeys();
   
   public abstract T compute();
}
