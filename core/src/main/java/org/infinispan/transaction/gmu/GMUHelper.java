package org.infinispan.transaction.gmu;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.gmu.GMUDataContainer;
import org.infinispan.container.gmu.GMUDataContainer.DataContainerVersionBody;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUDistributedVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.FlagsWrapper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUHelper {

   private static final Log log = LogFactory.getLog(GMUHelper.class);

   public static void performReadSetValidation(GMUPrepareCommand prepareCommand,
         DataContainer dataContainer,
         ClusteringDependentLogic keyLogic) {
      GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
      if (prepareCommand.getReadSet() == null || prepareCommand.getReadSet().length == 0) {
         if (log.isDebugEnabled()) {
            log.debugf("Validation of [%s] OK. no read set", gtx.prettyPrint());
         }
         return;
      }
      EntryVersion prepareVersion = prepareCommand.getPrepareVersion();
      for (Object key : prepareCommand.getReadSet()) {
         if (keyLogic.localNodeIsOwner(key)) {
            InternalCacheEntry cacheEntry = dataContainer.get(key, null); //get the most recent
            EntryVersion currentVersion = cacheEntry.getVersion();
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: Compare %s vs %s", gtx.prettyPrint(), key, currentVersion, prepareVersion);
            }
            if (currentVersion == null) {
               //this should only happens if the key does not exits. However, this can create some
               //consistency issues when eviction is enabled
               continue;
            }
            if (currentVersion.compareTo(prepareVersion) == InequalVersionComparisonResult.AFTER) {
               throw new ValidationException("Validation failed for key [" + key + "]", key);
            }
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: keys is not local", gtx.prettyPrint(), key);
            }
         }
      }
   }

   public static void performSSIReadSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand,
         DataContainer dataContainer, ClusteringDependentLogic keyLogic, GMUVersion latestVersion) {
      
      GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
      if (prepareCommand.getReadSet() == null || prepareCommand.getReadSet().length == 0) {
         if (log.isDebugEnabled()) {
            log.debugf("Validation of [%s] OK. no read set", gtx.prettyPrint());
         }
         return;
      }
      
      CacheTransaction cacheTx = context.getCacheTransaction();
      GMUDataContainer container = (GMUDataContainer) dataContainer;
      EntryVersion prepareVersion = prepareCommand.getPrepareVersion();
      long[] depVersion = new long[toGMUVersion(prepareVersion).getViewSize()];
      Arrays.fill(depVersion, Long.MAX_VALUE);
      
      for (Object key : prepareCommand.getReadSet()) {
         if (keyLogic.localNodeIsOwner(key)) {

            container.markVisibleRead(key, latestVersion);
//            System.out.println(Thread.currentThread().getId() + "] marked visible read: " + key + " " + Arrays.toString(((GMUDistributedVersion)latestVersion).getVersions()));

            DataContainerVersionBody body = container.getFirstBody(key);
            while (body != null && !body.isOlderOrEquals(prepareVersion)) {
               if (body.hasOutgoingDep()) {
                  throw new ValidationException("Missed concurrent write, and its owner already has outgoing [" + key + "] with time " + Arrays.toString(body.getCreatorActualVersion()), key);
               }
               
               cacheTx.setHasOutgoingEdge(true);
               mergeMinVectorClocks(depVersion, body.getCreatorActualVersion());
               body = body.getPrevious();
//               System.out.println(Thread.currentThread().getId() + "] missed concurrent write: " + key + " " + Arrays.toString(body.getCreatorActualVersion()));
            }
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: keys is not local", gtx.prettyPrint(), key);
            }
         }
      }
      cacheTx.setComputedDepsVersion(depVersion);
   }
   
   public static void mergeMinVectorClocks(long[] orig, long[] update) {
      for (int i = 0; i < orig.length; i++) {
         if (update[i] < orig[i]) {
            orig[i] = update[i];
         }
      }
   }


   public static void performWriteSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand,
         DataContainer dataContainer, ClusteringDependentLogic distributionLogic) {

      GMUDataContainer container = (GMUDataContainer) dataContainer;
      GMUDistributedVersion snapshotUsed = (GMUDistributedVersion) prepareCommand.getPrepareVersion();
      for (WriteCommand writeCommand : prepareCommand.getModifications()) {
         for (Object key : writeCommand.getAffectedKeys()) {
            if (distributionLogic.localNodeIsOwner(key)) {
               if (container.wasReadSince(key, snapshotUsed)) {
//                  System.out.println(Thread.currentThread().getId() + "] write to " + key + " invalidated read");
                  context.getCacheTransaction().setHasIncomingEdge(true);
                  break;
               }
            }
         }
      }

   }

   public static EntryVersion calculateCommitVersion(EntryVersion mergedVersion, GMUVersionGenerator versionGenerator,
         Collection<Address> affectedOwners) {
      if (mergedVersion == null) {
         throw new NullPointerException("Null merged version is not allowed to calculate commit view");
      }

      return versionGenerator.calculateCommitVersion(mergedVersion, affectedOwners);
   }

   public static InternalGMUCacheEntry toInternalGMUCacheEntry(InternalCacheEntry entry) {
      return convert(entry, InternalGMUCacheEntry.class);
   }

   public static GMUVersion toGMUVersion(EntryVersion version) {
      return convert(version, GMUVersion.class);
   }

   public static GMUVersionGenerator toGMUVersionGenerator(VersionGenerator versionGenerator) {
      return convert(versionGenerator, GMUVersionGenerator.class);
   }

   public static <T> T convert(Object object, Class<T> clazz) {
      if (log.isDebugEnabled()) {
         log.debugf("Convert object %s to class %s", object, clazz.getCanonicalName());
      }
      try {
         return clazz.cast(object);
      } catch (ClassCastException cce) {
         log.fatalf(cce, "Error converting object %s to class %s", object, clazz.getCanonicalName());
         throw new IllegalArgumentException("Expected " + clazz.getSimpleName() +
               " and not " + object.getClass().getSimpleName());
      }
   }

   public static void joinAndSetTransactionVersion(Collection<Response> responses, TxInvocationContext ctx,
         GMUVersionGenerator versionGenerator) {
      if (responses.isEmpty()) {
         if (log.isDebugEnabled()) {
            log.debugf("Versions received are empty!");
         }
         return;
      }
      List<EntryVersion> allPreparedVersions = new LinkedList<EntryVersion>();
      allPreparedVersions.add(ctx.getTransactionVersion());
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      //process all responses
      for (Response r : responses) {
         if (r == null) {
            throw new IllegalStateException("Non-null response with new version is expected");
         } else if (r instanceof SuccessfulResponse) {
            EntryVersion version = convert(((SuccessfulResponse) r).getResponseValue(), EntryVersion.class);
            allPreparedVersions.add(version);
         } else if(r instanceof ExceptionResponse) {
            throw new ValidationException(((ExceptionResponse) r).getException());
         } else if(!r.isSuccessful()) {
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.prettyPrint());
         }
      }

      EntryVersion[] preparedVersionsArray = new EntryVersion[allPreparedVersions.size()];
      EntryVersion commitVersion = versionGenerator.mergeAndMax(allPreparedVersions.toArray(preparedVersionsArray));

      if (log.isTraceEnabled()) {
         log.tracef("Merging transaction [%s] prepare versions %s ==> %s", gtx.prettyPrint(), allPreparedVersions,
               commitVersion);
      }

      ctx.setTransactionVersion(commitVersion);
   }

   public static void joinAndSetTransactionVersionAndFlags(Collection<Response> responses, 
         TxInvocationContext ctx, GMUVersionGenerator versionGenerator) {
      if (responses.isEmpty()) {
         if (log.isDebugEnabled()) {
            log.debugf("Versions received are empty!");
         }
         
         CacheTransaction cacheTx = ctx.getCacheTransaction();
         if (cacheTx.isHasIncomingEdge() && cacheTx.isHasOutgoingEdge()) {
//            System.out.println(Thread.currentThread().getId() + "] both edges exist");
            throw new ValidationException("Both edges exist", null);
         }
         
         long[] computedDeps = cacheTx.getComputedDepsVersion();
         if (wasNotComputed(computedDeps)) {
            cacheTx.setComputedDepsVersion(((GMUDistributedVersion)cacheTx.getTransactionVersion()).getVersions());
         }
//         System.out.println(Thread.currentThread().getId() + "] Alone commit time: " + Arrays.toString(((GMUDistributedVersion)cacheTx.getTransactionVersion()).getVersions()) + " computed deps: " + Arrays.toString(cacheTx.getComputedDepsVersion()));
         return;
      }
      List<EntryVersion> allPreparedVersions = new LinkedList<EntryVersion>();
      FlagsWrapper flagsWrapper = ctx.getPrepareResult();
      allPreparedVersions.add(flagsWrapper.getCreationVersion());
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      boolean outFlag = flagsWrapper.isHasOutgoingEdge();
      boolean inFlag = flagsWrapper.isHasIncomingEdge();
      long[] outDep = flagsWrapper.getComputedDepsVersion();
      
      //process all responses
      for (Response r : responses) {
         if (r == null) {
            throw new IllegalStateException("Non-null response with new version is expected");
         } else if (r instanceof SuccessfulResponse) {
            flagsWrapper = convert(((SuccessfulResponse) r).getResponseValue(), FlagsWrapper.class);
            allPreparedVersions.add(flagsWrapper.getCreationVersion());
            if (flagsWrapper.isHasIncomingEdge()) {
               inFlag = true;
            }
            if (flagsWrapper.isHasOutgoingEdge()) {
               outFlag = true;
            }
            mergeMinVectorClocks(outDep, flagsWrapper.getComputedDepsVersion());
         } else if(r instanceof ExceptionResponse) {
            throw new ValidationException(((ExceptionResponse) r).getException());
         } else if(!r.isSuccessful()) {
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.prettyPrint());
         }
      }

      if (outFlag && inFlag) {
         throw new ValidationException("Both edges exist", null);
      }
      
      EntryVersion[] preparedVersionsArray = new EntryVersion[allPreparedVersions.size()];
      EntryVersion commitVersion = versionGenerator.mergeAndMax(allPreparedVersions.toArray(preparedVersionsArray));

      if (log.isTraceEnabled()) {
         log.tracef("Merging transaction [%s] prepare versions %s ==> %s", gtx.prettyPrint(), allPreparedVersions,
               commitVersion);
      }

      GMUDistributedVersion distVersion = (GMUDistributedVersion) commitVersion;
      
      CacheTransaction cacheTx = ctx.getCacheTransaction();
      cacheTx.setHasOutgoingEdge(outFlag);
      cacheTx.setTransactionVersion(distVersion);
      if (wasNotComputed(outDep)) {
         cacheTx.setComputedDepsVersion(distVersion.getVersions());
      } else {
         cacheTx.setComputedDepsVersion(outDep);
      }
      
//      System.out.println(Thread.currentThread().getId() + "] 2PC commit time: " + Arrays.toString(((GMUDistributedVersion)cacheTx.getTransactionVersion()).getVersions()) + " computed deps: " + Arrays.toString(cacheTx.getComputedDepsVersion()));
   }
   
   private static boolean wasNotComputed(long[] versions) {
      boolean shouldReturnTrue = versions[0] == Long.MAX_VALUE;
      for (int i = 1; i < versions.length; i++) {
         if (versions[i] == Long.MAX_VALUE && !shouldReturnTrue) {
            throw new RuntimeException("Inconsistency in versions");
         } else if (versions[i] != Long.MAX_VALUE && shouldReturnTrue) {
            throw new RuntimeException("Inconsistency in versions");
         }
      }
      return shouldReturnTrue;
   }
   
}
