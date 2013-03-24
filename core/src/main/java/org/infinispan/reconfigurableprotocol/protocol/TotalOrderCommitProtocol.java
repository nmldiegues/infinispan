package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.CacheException;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.gmu.SSITotalOrderGMUEntryWrappingInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUDistributionInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUEntryWrappingInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUReplicationInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderReplicationInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderStateTransferLockInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedReplicationInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Total Order based replication is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "TO";

   private static final String TWO_PC_UID = TwoPhaseCommitProtocol.UID;

   private TransactionTable transactionTable;

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public final boolean canSwitchTo(ReconfigurableProtocol protocol) {
      return TWO_PC_UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public final void switchTo(ReconfigurableProtocol protocol) {
      manager.safeSwitch(protocol);
   }

   @Override
   public final void stopProtocol(boolean abortOnStop) throws InterruptedException {
      globalStopProtocol(true, abortOnStop);
   }

   @Override
   public final void bootProtocol() {
      //no-op
   }

   @Override
   public final void processTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet) {
      logProcessTransaction(globalTransaction);
   }

   @Override
   public final void processOldTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                           ReconfigurableProtocol currentProtocol) {
      try {
         throwOldTxException(globalTransaction);
      } catch (CacheException ce) {
         addException(ce, globalTransaction);
         throw ce;
      }
   }

   @Override
   public final void processSpeculativeTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                                   ReconfigurableProtocol oldProtocol) {
      try{
         logProcessSpeculativeTransaction(globalTransaction, oldProtocol);
         if (TWO_PC_UID.equals(oldProtocol.getUniqueProtocolName())) {
            try {
               oldProtocol.ensureNoConflict(writeSet);
               return;
            } catch (InterruptedException e) {
               //no-op
            }
         }

         throwSpeculativeTxException(globalTransaction);
      } catch (CacheException ce) {
         addException(ce, globalTransaction);
         throw ce;
      } catch (Exception e) {
         addException(e, globalTransaction);
         throw new CacheException(e);
      }
   }

   @Override
   public final void bootstrapProtocol() {
      transactionTable = getComponent(TransactionTable.class);
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //State transfer
      interceptors.put(InterceptorType.STATE_TRANSFER,
                       createInterceptor(new TotalOrderStateTransferLockInterceptor(),
                                         TotalOrderStateTransferLockInterceptor.class));

      //Custom interceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_BEFORE_TX_INTERCEPTOR,
                       createInterceptor(new TotalOrderInterceptor(), TotalOrderInterceptor.class));

      //No locking
      interceptors.remove(InterceptorType.LOCKING);

      //Wrapper
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         if (configuration.transaction().ssiValidation()) {
System.out.println("\n\n\n~~~~~  TOTAL ORDER TWM ~~~~~~\n\n\n");
            interceptors.put(InterceptorType.WRAPPER,
                  createInterceptor(new SSITotalOrderGMUEntryWrappingInterceptor(),
                        SSITotalOrderGMUEntryWrappingInterceptor.class));            
         } else {
System.out.println("\n\n\n~~~~~  TOTAL ORDER GMU ~~~~~~\n\n\n");
            interceptors.put(InterceptorType.WRAPPER,
                  createInterceptor(new TotalOrderGMUEntryWrappingInterceptor(),
                        TotalOrderGMUEntryWrappingInterceptor.class));
         }
      } else if (needsVersionAwareComponents()) {
         interceptors.put(InterceptorType.WRAPPER,
                          createInterceptor(new TotalOrderVersionedEntryWrappingInterceptor(),
                                            TotalOrderVersionedEntryWrappingInterceptor.class));
      }

      //No deadlock
      interceptors.remove(InterceptorType.DEADLOCK);

      //Clustering
      switch (configuration.clustering().cacheMode()) {
         case REPL_SYNC:
            if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderGMUReplicationInterceptor(),
                                                  TotalOrderGMUReplicationInterceptor.class));
            } else if (needsVersionAwareComponents()) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderVersionedReplicationInterceptor(),
                                                  TotalOrderVersionedReplicationInterceptor.class));
               break;
            } else {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderReplicationInterceptor(),
                                                  TotalOrderReplicationInterceptor.class));
               break;
            }
         case DIST_SYNC:
            if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderGMUDistributionInterceptor(),
                                                  TotalOrderGMUDistributionInterceptor.class));
            } else if (needsVersionAwareComponents()) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderVersionedDistributionInterceptor(),
                                                  TotalOrderVersionedDistributionInterceptor.class));
               break;
            } else {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderDistributionInterceptor(),
                                                  TotalOrderDistributionInterceptor.class));
               break;
            }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Building interceptor chain for Total Order protocol %s", interceptors);
      }

      return interceptors;
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      //force always one phase commit for total order protocol
      //return true;
      //original condition:
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         return false;
      } else if (!configuration.versioning().enabled()) {
         return true;
      }
      return configuration.transaction().useSynchronization() && !configuration.clustering().cacheMode().isDistributed();
   }

   @Override
   public final boolean useTotalOrder() {
      return true;
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      //no-op
   }

   private void addException(Exception e, GlobalTransaction globalTransaction) {
      //this is not the most efficient way to do it, but it should have a lower number of local transactions
      for (LocalTransaction localTransaction : transactionTable.getLocalTransactions()) {
         if (localTransaction.getGlobalTransaction().equals(globalTransaction)) {
            //TODO revisit this!
            //localTransaction.addException(e, true);
            break;
         }
      }
   }
}
