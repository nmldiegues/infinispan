package org.infinispan.container.gmu;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUValueCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;
import static org.infinispan.transaction.gmu.GMUHelper.toInternalGMUCacheEntry;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUEntryFactoryImpl extends EntryFactoryImpl {

   private static final Log log = LogFactory.getLog(GMUEntryFactoryImpl.class);
   private CommitLog commitLog;
   private GMUVersionGenerator gmuVersionGenerator;

   public static InternalGMUCacheEntry wrap(Object key, InternalCacheEntry entry, boolean mostRecent, boolean sawOutgoing,
         EntryVersion maxTxVersion, EntryVersion creationVersion,
         EntryVersion maxValidVersion) {
      if (entry == null || entry.isNull()) {
         return new InternalGMUNullCacheEntry(key, (entry == null ? null : entry.getVersion()), maxTxVersion, mostRecent, sawOutgoing,
               creationVersion, maxValidVersion);
      }
      return new InternalGMUValueCacheEntry(entry, maxTxVersion, mostRecent, sawOutgoing, creationVersion, maxValidVersion);
   }

   @Inject
   public void injectDependencies(CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.gmuVersionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   public void start() {
      useRepeatableRead = false;
      localModeWriteSkewCheck = false;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, Object value, EntryVersion version, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) {
         return forRemoval ? new NullMarkerEntryForRemoval(key, version) : NullMarkerEntry.getInstance();
      }
      return new SerializableEntry(key, value, lifespan, version);
   }

   public InternalCacheEntry getDelayedFromContainer(Object key, InvocationContext context) {
      EntryVersion maxVersionToRead = commitLog.getAvailableVersionLessThan(null);
      InternalGMUCacheEntry entry = toInternalGMUCacheEntry(container.get(key, maxVersionToRead));
      return entry.getInternalCacheEntry();
   }
   
   @Override
   protected InternalCacheEntry getFromContainer(Object key, InvocationContext context, GetKeyValueCommand command) {
      boolean singleRead = context instanceof SingleKeyNonTxInvocationContext;
      boolean remotePrepare = !context.isOriginLocal() && context.isInTxScope();
      boolean remoteRead = !context.isOriginLocal() && !context.isInTxScope();

      EntryVersion versionToRead;
      if (singleRead || remotePrepare) {
         //read the most recent version
         //in the prepare, the value does not matter (it will be written or it is not read)
         //                and the version does not matter either (it will be overwritten)
         versionToRead = null;
      } else {
         versionToRead = context.calculateVersionToRead(gmuVersionGenerator);
      }

      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      if (context.isInTxScope() && context.isOriginLocal() && !context.hasAlreadyReadOnThisNode()) {
         //firs read on the local node for a transaction. ensure the min version
         EntryVersion transactionVersion = ((TxInvocationContext) context).getTransactionVersion();
         try {
            commitLog.waitForVersion(transactionVersion, -1);
         } catch (InterruptedException e) {
            //ignore...
         }
      }

      EntryVersion maxVersionToRead = hasAlreadyReadFromThisNode ? versionToRead :
         commitLog.getAvailableVersionLessThan(versionToRead);

      GMUVersion beginVC = context.getBeginVC();
      if (beginVC == null) {
         if (context instanceof TxInvocationContext) {
            beginVC = ((TxInvocationContext)context).getCacheTransaction().getBeginVC();
         }
         if (beginVC == null) {
            beginVC = TransactionCommitManager.singleton.getLastCommittedVC();
         }
      }
      EntryVersion mostRecentCommitLogVersion = commitLog.getCurrentVersion();

      InternalGMUCacheEntry entry = null;
      LocalTransaction localTx = context.getLocalTransaction();
      
      if (localTx != null && localTx.isWriteTx()) {
         // we have local transaction and it is a writer. we are definitely using SSI
         entry = toInternalGMUCacheEntry(((GMUDataContainer)container).getAsWriteTx(key, maxVersionToRead, beginVC));
         
      } else if (localTx != null && !localTx.isWriteTx() && configuration.isSSIValidation()) {
         // we have local transaction, it's read only and we are using SSI
         entry = toInternalGMUCacheEntry(((GMUDataContainer)container).getAsRO(key, maxVersionToRead, beginVC));
         
      }
      else if (command != null && command.hasFlag(Flag.WRITE_TX)) {
         // remote command with writer transaction, definitely SSI
         entry = toInternalGMUCacheEntry(((GMUDataContainer)container).getAsWriteTx(key, maxVersionToRead, beginVC));
         
      } else if (command != null && !command.hasFlag(Flag.WRITE_TX) && configuration.isSSIValidation()) {
         // remote command with read-only and SSI
         entry = toInternalGMUCacheEntry(((GMUDataContainer)container).getAsRO(key, maxVersionToRead, beginVC));
         
      }
      else {
         // any other case, read normally
         entry = toInternalGMUCacheEntry(container.get(key, maxVersionToRead));
      }

      if (remoteRead) {
         if (entry.getMaximumValidVersion() == null) {
            entry.setMaximumValidVersion(mostRecentCommitLogVersion);
         } else {
            entry.setMaximumValidVersion(commitLog.getEntry(entry.getMaximumValidVersion()));
         }
         if (entry.getCreationVersion() == null) {
            entry.setCreationVersion(commitLog.getOldestVersion());
         } else {
            entry.setCreationVersion(commitLog.getEntry(entry.getCreationVersion()));
         }
      }

      context.addKeyReadInCommand(key, entry);

      if (log.isTraceEnabled()) {
         log.tracef("Retrieved from container %s", entry);
      }

      return entry.getInternalCacheEntry();
   }
}
