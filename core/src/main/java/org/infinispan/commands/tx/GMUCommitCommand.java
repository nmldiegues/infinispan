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
package org.infinispan.commands.tx;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Command corresponding to the 2nd phase of 2PC when serializable isolation level is used
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUCommitCommand extends CommitCommand {
   public static final byte COMMAND_ID = 103;   

   private EntryVersion commitVersion;
   private long[] computedDepsVersion;
   private boolean outgoing;
   private boolean[] boostedIndexes;

   public GMUCommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public GMUCommitCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "GMUCommitCommand {" + 
            "commitVersion=" + commitVersion +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }

   public void setCommitVersion(EntryVersion commitVersion) {
      this.commitVersion = commitVersion;
   }

   public EntryVersion getCommitVersion() {
      return commitVersion;
   }
   
   public boolean[] getBoostedIndexes() {
      return this.boostedIndexes;
   }
   
   public void setBoostedIndexes(boolean[] boosts) {
      this.boostedIndexes = boosts;
   }
   
   public void setOutgoing(boolean outgoing) {
      this.outgoing = outgoing;
   }
   
   public boolean isOutgoing() {
      return this.outgoing;
   }

   public void setComputedDepsVersion(long[] computedDepsVersion) {
      this.computedDepsVersion = computedDepsVersion;
   }
   
   public long[] getComputedDepsVersion() {
      return this.computedDepsVersion;
   }
   
   @Override
   public Object[] getParameters() {
      return new Object[] {globalTx, commitVersion, computedDepsVersion, outgoing, boostedIndexes};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      commitVersion = (EntryVersion) args[1];
      computedDepsVersion = (long[]) args[2];
      outgoing = (Boolean) args[3];
      boostedIndexes = (boolean[]) args[4];
   }
}
