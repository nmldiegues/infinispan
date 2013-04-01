/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.util.concurrent.locks.OwnableReentrantLock;

import java.util.concurrent.TimeUnit;

/**
 * A per-entry lock container for OwnableReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class OwnableReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<OwnableReentrantLock> {

   public OwnableReentrantPerEntryLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   public boolean ownsExclusiveLock(Object key, Object owner) {
      OwnableReentrantLock l = getLockFromMap(key, false);
      return l != null && owner.equals(l.getOwner());
   }

   @Override
   public boolean ownsShareLock(Object key, Object owner) {
      return false;
   }

   @Override
   public boolean isExclusiveLocked(Object key) {
      OwnableReentrantLock l = getLockFromMap(key, false);
      return l != null && l.isLocked();
   }

   @Override
   public boolean isSharedLocked(Object key) {
      return false;
   }

   @Override
   public OwnableReentrantLock getExclusiveLock(Object key) {
      return getLockFromMap(key, true);
   }

   @Override
   public OwnableReentrantLock getShareLock(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   protected OwnableReentrantLock newLock() {
      return new OwnableReentrantLock();
   }
   
   @Override
   protected boolean tryExclusiveLock(OwnableReentrantLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected boolean tryShareLock(OwnableReentrantLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return false;
   }

   @Override
   protected void unlockExclusive(OwnableReentrantLock l, Object owner) {
      l.unlock(owner);
   }

   @Override
   protected void unlockShare(OwnableReentrantLock toRelease, Object ctx) {
      //no-op
   }
}
