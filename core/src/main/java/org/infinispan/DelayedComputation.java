package org.infinispan;

import java.io.Serializable;
import java.util.Collection;


public interface DelayedComputation<T> extends Serializable {

   public abstract Collection<Object> getAffectedKeys();
   
   public abstract T compute();
}
