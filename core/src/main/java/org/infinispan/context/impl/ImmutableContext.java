/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.context.impl;

import org.infinispan.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This context is a non-context for operations such as eviction which are not related
 * to the method invocation which caused them.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 */
public final class ImmutableContext implements InvocationContext {

   public static final ImmutableContext INSTANCE = new ImmutableContext();

   private ImmutableContext() {
      //don't create multiple instances
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      throw newUnsupportedMethod();
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return Collections.emptyMap();
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      throw newUnsupportedMethod();
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      throw newUnsupportedMethod();
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      throw newUnsupportedMethod();
   }

   @Override
   public void clearLookedUpEntries() {
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return false;
   }

   @Override
   public boolean hasFlag(Flag o) {
      return false;
   }

   @Override
   public Set<Flag> getFlags() {
      return Collections.emptySet();
   }

   @Override
   public void setFlags(Flag... flags) {
      throw newUnsupportedMethod();
   }

   @Override
   public void setFlags(Collection<Flag> flags) {
      throw newUnsupportedMethod();
   }

   @Override
   public void reset() {
   }

   @Override
   public boolean isOriginLocal() {
      return true;
   }

   @Override
   public Address getOrigin() {
      return null;
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return null;
   }

   @Override
   public boolean isUseFutureReturnType() {
      return false;
   }

   @Override
   public void setUseFutureReturnType(boolean useFutureReturnType) {
      throw newUnsupportedMethod();
   }

   @Override
   public Set<Object> getLockedKeys() {
      return Collections.emptySet();
   }

   @Override
   public InvocationContext clone() {
      return this;
   }

   @Override
   public ClassLoader getClassLoader() {
      return null;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      throw newUnsupportedMethod();
   }

   /**
    * @return an exception to state this context is read only
    */
   private static CacheException newUnsupportedMethod() {
      return new CacheException("This context is immutable");
   }

   @Override
   public void addLockedKey(Object key) {
      throw new CacheException("This context is immutable");
   }

   @Override
   public void clearLockedKeys() {
      throw new CacheException("This context is immutable");
   }

   @Override
   public void addKeyReadInCommand(Object key, InternalGMUCacheEntry entry) {
      //no-op
   }

   @Override
   public void clearKeyReadInCommand() {
      //no-op
   }

   @Override
   public Map<Object, InternalGMUCacheEntry> getKeysReadInCommand() {
      return Collections.emptyMap();
   }

   @Override
   public EntryVersion calculateVersionToRead(VersionGenerator versionGenerator) {
      return null;
   }

   @Override
   public void setVersionToRead(EntryVersion entryVersion) {
      //no-op
   }

   @Override
   public boolean hasAlreadyReadOnThisNode() {
      return false;
   }

   @Override
   public void setAlreadyReadOnThisNode(boolean value) {
      //no-op
   }

   @Override
   public void setProtocolId(String protocolId) {
      throw newUnsupportedMethod();
   }

   @Override
   public String getProtocolId() {
      throw newUnsupportedMethod();
   }
   
   @Override
   public LocalTransaction getLocalTransaction() {
      throw newUnsupportedMethod();
   }

   @Override
   public void setBeginVC(GMUVersion beginVC) {
      throw newUnsupportedMethod();
   }

   @Override
   public GMUVersion getBeginVC() {
      throw newUnsupportedMethod();
   }
}
