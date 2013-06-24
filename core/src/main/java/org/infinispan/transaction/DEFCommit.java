package org.infinispan.transaction;

import java.io.Serializable;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;

public class DEFCommit implements DistributedCallable, Serializable {

   private GlobalTransaction tx;
   private Cache cache;

   public DEFCommit(GlobalTransaction tx) {
      this.tx = tx;
   }

   @Override
   public Object call() throws Exception {
      LocalTransaction local = this.cache.getAdvancedCache().getTxTable().getLocalTransaction(tx);
      DummyTransactionManager tm = (DummyTransactionManager) this.cache.getAdvancedCache().getTransactionManager();
      javax.transaction.Transaction jpaTx = local.getTransaction();
      tm.resume(jpaTx);
      try {
         tm.commitOrder();
      } finally {
         tm.suspend();
      }
      return null;
   }

   @Override
   public void setEnvironment(Cache cache, Set inputKeys) {
      this.cache = cache;
   }

}