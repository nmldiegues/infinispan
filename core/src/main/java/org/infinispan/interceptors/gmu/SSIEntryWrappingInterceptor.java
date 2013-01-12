package org.infinispan.interceptors.gmu;

import static org.infinispan.transaction.gmu.GMUHelper.calculateCommitVersion;
import static org.infinispan.transaction.gmu.GMUHelper.convert;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Nuno Diegues
 * @since 5.2
 */
public class SSIEntryWrappingInterceptor extends GMUEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(SSIEntryWrappingInterceptor.class);
   
   @Override
   protected void shouldEarlyAbort(TxInvocationContext txInvocationContext, InternalGMUCacheEntry internalGMUCacheEntry) {
      // empty on purpose, SSI never aborts because of a single stale read
   }
   
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      GMUPrepareCommand spc = convert(command, GMUPrepareCommand.class);

      if (ctx.isOriginLocal()) {
         spc.setVersion(ctx.getTransactionVersion());
         spc.setReadSet(ctx.getReadSet());
      } else {
         ctx.setTransactionVersion(spc.getPrepareVersion());
      }

      wrapEntriesForPrepare(ctx, command);
      performValidation(ctx, spc);

      Object retVal = invokeNextInterceptor(ctx, command);

      if (ctx.isOriginLocal() && command.getModifications().length > 0) {
         EntryVersion commitVersion = calculateCommitVersion(ctx.getTransactionVersion(), versionGenerator,
                                                             cll.getWriteOwners(ctx.getCacheTransaction()));
         ctx.setTransactionVersion(commitVersion);
      } else {
         // The following is different from the super class
         retVal = ctx.getPrepareResult();
      }

      if (command.isOnePhaseCommit()) {
         commitContextEntries.commitContextEntries(ctx);
      }

      return retVal;
   }
   
   @Override
   protected void performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      boolean hasToUpdateLocalKeys = false;
      boolean isReadOnly = command.getModifications().length == 0;

      for (Object key : command.getAffectedKeys()) {
         if (cll.localNodeIsOwner(key)) {
            hasToUpdateLocalKeys = true;
            break;
         }
      }

      if (!hasToUpdateLocalKeys) {
         for (WriteCommand writeCommand : command.getModifications()) {
            if (writeCommand instanceof ClearCommand) {
               hasToUpdateLocalKeys = true;
               break;
            }
         }
      }

      cll.performWriteSetValidation(ctx, command);
      
      if (!isReadOnly) {
         cll.performSSIReadSetValidation(ctx, command, commitLog.getCurrentVersion());
         if (hasToUpdateLocalKeys) {
            transactionCommitManager.prepareTransaction(ctx.getCacheTransaction());
         } else {
            transactionCommitManager.prepareReadOnlyTransaction(ctx.getCacheTransaction());
         }
      }

      if (log.isDebugEnabled()) {
         log.debugf("Transaction %s can commit on this node. Prepare Version is %s",
                    command.getGlobalTransaction().prettyPrint(), ctx.getTransactionVersion());
      }
      
   }
   
}
