package org.infinispan.interceptors.gmu;

import static org.infinispan.transaction.gmu.GMUHelper.calculateCommitVersion;
import static org.infinispan.transaction.gmu.GMUHelper.convert;

import java.util.Set;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RequestHandler;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SSITotalOrderGMUEntryWrappingInterceptor extends TotalOrderGMUEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(SSITotalOrderGMUEntryWrappingInterceptor.class);

   @Override
   protected void shouldEarlyAbort(TxInvocationContext txInvocationContext, InternalGMUCacheEntry internalGMUCacheEntry) {
      if (txInvocationContext.getLocalTransaction().isWriteTx() && !internalGMUCacheEntry.isMostRecent() && internalGMUCacheEntry.sawOutgoing()) {
         throw new CacheException("Read-Write transaction read an old value produced by a time-warp committed tx and should rollback");
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      GMUPrepareCommand spc = convert(command, GMUPrepareCommand.class);

      if (ctx.isOriginLocal()) {
         spc.setVersion(ctx.getTransactionVersion());
         spc.setReadSet(ctx.getReadSet());
         spc.setBeginVC(GMUHelper.LAST_COMMIT_VC.get());
      } else {
         ctx.setTransactionVersion(spc.getPrepareVersion());
         ctx.getCacheTransaction().setBeginVC(spc.getBeginVC());
      }

      wrapEntriesForPrepare(ctx, command);
      performValidation(ctx, spc);

      Object retVal = invokeNextInterceptor(ctx, command);

      if (ctx.isOriginLocal()) {
         EntryVersion commitVersion = calculateCommitVersion(ctx.getTransactionVersion(), versionGenerator,
               cll.getWriteOwners(ctx.getCacheTransaction()));
         ctx.setTransactionVersion(commitVersion);
      } else {
         retVal = ctx.getPrepareResult();
      }

      if (command.isOnePhaseCommit()) {
         commitContextEntries.commitContextEntries(ctx);
      }

      return retVal;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GMUCommitCommand gmuCommitCommand = convert(command, GMUCommitCommand.class);

      if (ctx.isOriginLocal()) {
         gmuCommitCommand.setCommitVersion(ctx.getTransactionVersion());
         gmuCommitCommand.setComputedDepsVersion(ctx.getCacheTransaction().getComputedDepsVersion());
         gmuCommitCommand.setOutgoing(ctx.getCacheTransaction().isHasOutgoingEdge());
      } else {
         ctx.setTransactionVersion(gmuCommitCommand.getCommitVersion());
         ctx.getCacheTransaction().setComputedDepsVersion(gmuCommitCommand.getComputedDepsVersion());
         ctx.getCacheTransaction().setHasOutgoingEdge(gmuCommitCommand.isOutgoing());
      }

      transactionCommitManager.commitTransaction(ctx.getCacheTransaction(), gmuCommitCommand.getCommitVersion(), 
            gmuCommitCommand.getComputedDepsVersion(), gmuCommitCommand.isOutgoing());

      Object retVal = null;
      try {
         retVal = invokeNextInterceptor(ctx, command);
      } catch (Throwable throwable) {
         //let ignore the exception. we cannot have some nodes applying the write set and another not another one
         //receives the rollback and don't applies the write set
      } finally {
         transactionCommitManager.awaitUntilCommitted(ctx.getCacheTransaction(), ctx.isOriginLocal() ? null : gmuCommitCommand);
      }
      return ctx.isOriginLocal() ? retVal : RequestHandler.DO_NOT_REPLY;
   }

   @Override
   protected void performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      if (!ctx.isOriginLocal()) {
         boolean hasToUpdateLocalKeys = false;

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

         Set<Object> readSet = cll.performSSIReadSetValidation(ctx, command);
         cll.performWriteSetValidation(ctx, command, readSet);
         
         if (hasToUpdateLocalKeys) {
            transactionCommitManager.prepareTransaction(ctx.getCacheTransaction());
         } else {
            transactionCommitManager.prepareReadOnlyTransaction(ctx.getCacheTransaction());
         }

         if (log.isDebugEnabled()) {
            log.debugf("Transaction %s can commit on this node. Prepare Version is %s",
                  command.getGlobalTransaction().prettyPrint(), ctx.getTransactionVersion());
         }
      }
   }

}
