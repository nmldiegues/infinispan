package org.infinispan.interceptors.gmu;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RequestHandler;

import java.util.LinkedList;
import java.util.List;

import static org.infinispan.transaction.gmu.GMUHelper.*;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUEntryWrappingInterceptor extends EntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(GMUEntryWrappingInterceptor.class);
   protected GMUVersionGenerator versionGenerator;
   protected TransactionCommitManager transactionCommitManager;
   protected CommitLog commitLog;

   @Inject
   public void inject(TransactionCommitManager transactionCommitManager, DataContainer dataContainer,
                      CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.transactionCommitManager = transactionCommitManager;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
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
         retVal = ctx.getTransactionVersion();
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
      } else {
         ctx.setTransactionVersion(gmuCommitCommand.getCommitVersion());
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
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         transactionCommitManager.rollbackTransaction(ctx.getCacheTransaction());
      }
   }

   /*
    * NOTE: these are the only commands that passes values to the application and these keys needs to be validated
    * and added to the transaction read set.
    */

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitGetKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitPutKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitRemoveCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitReplaceCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   /**
    * validates the read set and returns the prepare version from the commit queue
    *
    * @param ctx     the context
    * @param command the prepare command
    * @throws InterruptedException if interrupted
    */
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

      if (!isReadOnly) {
         cll.performReadSetValidation(ctx, command);
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

   private void updateTransactionVersion(InvocationContext context) {
      if (!context.isInTxScope() && !context.isOriginLocal()) {
         return;
      }

      if (context instanceof SingleKeyNonTxInvocationContext) {
         if (log.isDebugEnabled()) {
            log.debugf("Received a SingleKeyNonTxInvocationContext... This should be a single read operation");
         }
         return;
      }

      TxInvocationContext txInvocationContext = (TxInvocationContext) context;
      List<EntryVersion> entryVersionList = new LinkedList<EntryVersion>();
      entryVersionList.add(txInvocationContext.getTransactionVersion());

      if (log.isTraceEnabled()) {
         log.tracef("[%s] Keys read in this command: %s", txInvocationContext.getGlobalTransaction().prettyPrint(),
                    txInvocationContext.getKeysReadInCommand());
      }

      for (InternalGMUCacheEntry internalGMUCacheEntry : txInvocationContext.getKeysReadInCommand().values()) {
         Object key = internalGMUCacheEntry.getKey();
         boolean local = cll.localNodeIsOwner(key);
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Analyze entry [%s]: local?=%s",
                       txInvocationContext.getGlobalTransaction().prettyPrint(),
                       internalGMUCacheEntry, local);
         }
         
         shouldEarlyAbort(txInvocationContext, internalGMUCacheEntry);

         if (internalGMUCacheEntry.getMaximumTransactionVersion() != null) {
            entryVersionList.add(internalGMUCacheEntry.getMaximumTransactionVersion());
         }
         
         if (!txInvocationContext.hasFlag(Flag.READ_WITHOUT_REGISTERING)) {
             txInvocationContext.getCacheTransaction().addReadKey(key);
         }
         
         if (local) {
            txInvocationContext.setAlreadyReadOnThisNode(true);
            txInvocationContext.addReadFrom(cll.getAddress());
         }
      }

      if (entryVersionList.size() > 1) {
         EntryVersion[] txVersionArray = new EntryVersion[entryVersionList.size()];
         txInvocationContext.setTransactionVersion(versionGenerator.mergeAndMax(entryVersionList.toArray(txVersionArray)));
      }
   }

   protected void shouldEarlyAbort(TxInvocationContext txInvocationContext, InternalGMUCacheEntry internalGMUCacheEntry) {
      if (txInvocationContext.hasModifications() && !internalGMUCacheEntry.isMostRecent()) {
         throw new CacheException("Read-Write transaction read an old value and should rollback");
      }
   }

}
