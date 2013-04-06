package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Delegates the method invocations for the correct instance depending of the protocol, for the ClusteringDependentLogic
 * component
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusteringDependentLogicDelegate extends AbstractProtocolDependentComponent<ClusteringDependentLogic>
      implements ClusteringDependentLogic {


   @Override
   public boolean localNodeIsOwner(Object key) {
      return get().localNodeIsOwner(key);
   }

   @Override
   public boolean localNodeIsPrimaryOwner(Object key) {
      return get().localNodeIsPrimaryOwner(key);
   }

   @Override
   public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
      get().commitEntry(entry, newVersion, skipOwnershipCheck);
   }

   @Override
   public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
      return get().createNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
   }

   @Override
   public Address getAddress() {
      return get().getAddress();
   }

   @Override
   public Collection<Address> getInvolvedNodes(CacheTransaction cacheTransaction) {
      return get().getInvolvedNodes(cacheTransaction);
   }

   @Override
   public Set<Object> performReadSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand) {
      get().performReadSetValidation(context, prepareCommand);
      return Collections.emptySet();
   }

   @Override
   public Collection<Address> getWriteOwners(CacheTransaction cacheTransaction) {
      return get().getWriteOwners(cacheTransaction);
   }

   @Override
   public void performWriteSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand, Set<Object> readSet) {
      get().performWriteSetValidation(context, prepareCommand, readSet);
   }

   @Override
   public void refreshVisibleReads(GMUPrepareCommand prepareCommand, long currentPrepVersion) {
      get().refreshVisibleReads(prepareCommand, currentPrepVersion);
   }

   @Override
   public Set<Object> performSSIReadSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand) {
      return get().performSSIReadSetValidation(context, prepareCommand);
   }

}
