package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.interceptors.PassivationInterceptor;
import org.infinispan.interceptors.PassiveReplicationInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Passive Replication is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "PB";
   private static final String MASTER_ACK = "_MASTER_ACK_";
   private boolean masterAckReceived = false;

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public final boolean switchTo(ReconfigurableProtocol protocol) {
      return TwoPhaseCommitProtocol.UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public final void stopProtocol() throws InterruptedException {
      if (isCoordinator()) {
         awaitUntilLocalTransactionsFinished();
         broadcastData(MASTER_ACK, false);
      } else {
         synchronized (this) {
            while (!masterAckReceived) {
               this.wait();
            }
            masterAckReceived = false;
         }
      }
      //this wait should return immediately, because we don't have any remote transactions pending...
      //it is just to be safe
      awaitUntilRemoteTransactionsFinished();
   }

   @Override
   public final void bootProtocol() {
      //no-op
   }

   @Override
   public final boolean canProcessOldTransaction(GlobalTransaction globalTransaction) {
      return TwoPhaseCommitProtocol.UID.equals(globalTransaction.getReconfigurableProtocol().getUniqueProtocolName());
   }

   @Override
   public final void bootstrapProtocol() {
      //no-op
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //Custom interceptor after TxInterceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
                       createInterceptor(new PassivationInterceptor(), PassiveReplicationInterceptor.class));

      return interceptors;
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      return !configuration.versioning().enabled() || configuration.transaction().useSynchronization();
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      if (MASTER_ACK.equals(data)) {
         synchronized (this) {
            masterAckReceived = true;
            this.notifyAll();
         }
      }
   }
}
