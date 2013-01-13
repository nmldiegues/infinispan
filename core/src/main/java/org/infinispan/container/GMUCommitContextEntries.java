package org.infinispan.container;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUCommitContextEntries extends NonVersionedCommitContextEntries {

   private final Log log = LogFactory.getLog(GMUCommitContextEntries.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope()) {
         commitEntryPossibleSSI(entry, ctx, skipOwnershipCheck);
      } else {
         commitEntry(entry, entry.getVersion(), skipOwnershipCheck);
      }
   }
   
   protected void commitEntryPossibleSSI(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (configuration.transaction().ssiValidation()) {
         commitDistributedEntrySSI(entry, ((TxInvocationContext)ctx), skipOwnershipCheck);
      } else {
         super.commitEntry(entry, ((TxInvocationContext)ctx).getTransactionVersion(), skipOwnershipCheck);
      }
   }

   private void commitDistributedEntrySSI(CacheEntry entry, TxInvocationContext ctx, boolean skipOwnershipCheck) {
      boolean doCommit = true;
      boolean local = distributionManager.getLocality(entry.getKey()).isLocal();
      // ignore locality for removals, even if skipOwnershipCheck is not true
      if (!skipOwnershipCheck && !entry.isRemoved() && !distributionManager.getLocality(entry.getKey()).isLocal()) {
         if (configuration.clustering().l1().enabled()) {
            distributionManager.transformForL1(entry);
         } else {
            doCommit = false;
         }
      }
      Log log = getLog();
      if (log.isTraceEnabled()) {
         log.tracef("Trying to commit entry in distributed mode. commit?=%s, local?=%s, key=%s", doCommit, local,
                    entry.getKey());
      }
      if (doCommit) {
         entry.commitSSI(dataContainer, ctx);
      } else {
         entry.rollback();
      }
   }
}
