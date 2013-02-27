package org.infinispan.container;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NonVersionedCommitContextEntries implements CommitContextEntries {

   private final Log log = LogFactory.getLog(NonVersionedCommitContextEntries.class);
   protected Configuration configuration;
   protected DistributionManager distributionManager;
   protected DataContainer dataContainer;

   @Inject
   public void inject(Configuration configuration, DistributionManager distributionManager, DataContainer dataContainer) {
      this.configuration = configuration;
      this.distributionManager = distributionManager;
      this.dataContainer = dataContainer;
   }

   @Override
   public final void commitContextEntries(InvocationContext context) {
      final Log log = getLog();
      final boolean trace = log.isTraceEnabled();
      boolean skipOwnershipCheck = context.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);

      if (context instanceof SingleKeyNonTxInvocationContext) {
         CacheEntry entry = ((SingleKeyNonTxInvocationContext) context).getCacheEntry();
         commitEntryIfNeeded(context, skipOwnershipCheck, entry);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = context.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(context, skipOwnershipCheck, entry)) {
               if (trace) {
                  if (entry == null)
                     log.tracef("Entry for key %s is null : not calling commitUpdate", e.getKey());
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", e.getKey(), entry);
               }
            }
         }
      }
   }

   protected Log getLog() {
      return log;
   }

   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      commitEntry(ctx, entry, null, skipOwnershipCheck);
   }

   protected void commitEntry(InvocationContext ctx, CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
      if (configuration.clustering().cacheMode().isDistributed()) {
         commitDistributedEntry(ctx, entry, newVersion, skipOwnershipCheck);
      } else {
         commitReplicatedEntry(entry, newVersion);
      }
   }

   private boolean commitEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, CacheEntry entry) {
      Log log = getLog();
      if (entry != null && entry.isChanged()) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry has changed. Committing %s", entry);
         }
         commitContextEntry(entry, ctx, skipOwnershipCheck);
         if (log.isTraceEnabled()) {
            log.tracef("Committed entry %s", entry);
         }
         return true;
      }
      return false;
   }

   private void commitDistributedEntry(InvocationContext ctx, CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
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
         if (configuration.transaction().ssiValidation()) {
            entry.commitSSI(dataContainer, (TxInvocationContext) ctx);
         } else {
            entry.commit(dataContainer, newVersion);
         }
      } else {
         entry.rollback();
      }
   }

   private void commitReplicatedEntry(CacheEntry entry, EntryVersion newVersion) {
      Log log = getLog();
      if (log.isTraceEnabled()) {
         log.tracef("Trying to commit entry in replicated mode. key=%s", entry.getKey());
      }
      entry.commit(dataContainer, newVersion);
   }
}
