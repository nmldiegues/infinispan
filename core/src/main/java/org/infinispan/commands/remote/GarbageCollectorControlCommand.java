package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.gmu.manager.GarbageCollectorManager;
import org.infinispan.util.Util;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class GarbageCollectorControlCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 100;
   private Type type;
   private int minimumVisibleViewId;
   private GarbageCollectorManager garbageCollectorManager;

   public GarbageCollectorControlCommand(String cacheName, Type type, int minimumVisibleViewId) {
      super(cacheName);
      this.type = type;
      this.minimumVisibleViewId = minimumVisibleViewId;
   }

   public GarbageCollectorControlCommand(String cacheName) {
      super(cacheName);
   }

   public void init(GarbageCollectorManager garbageCollectorManager) {
      this.garbageCollectorManager = garbageCollectorManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case GET_VERSION:
            return garbageCollectorManager.handleGetMinimumVisibleVersion();
         case GET_VIEW_ID:
            return garbageCollectorManager.handleGetMinimumVisibleViewId();
         case SET_VIEW_ID:
            garbageCollectorManager.handleDeleteOlderViewId(minimumVisibleViewId);
            return null;
         default:
            throw new IllegalStateException("Type not found!");
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      switch (type) {
         case GET_VERSION:
         case GET_VIEW_ID:
            return new Object[]{typeToByte()};
         case SET_VIEW_ID:
            return new Object[]{typeToByte(), minimumVisibleViewId};
      }
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (getCommandId() != commandId) {
         throw new IllegalArgumentException("Command ID does not match! " + getCommandId() + " != " + commandId);
      }
      type = byteToType((Byte) parameters[0]);
      if (type == Type.SET_VIEW_ID) {
         minimumVisibleViewId = (Integer) parameters[1];
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return type == Type.GET_VIEW_ID || type == Type.GET_VERSION;
   }

   @Override
   public String toString() {
      return "GarbageCollectorControlCommand{" +
            "type=" + type +
            ", minimumVisibleViewId=" + minimumVisibleViewId +
            "} " + super.toString();
   }

   private byte typeToByte() {
      return (byte) type.ordinal();
   }

   private Type byteToType(byte value) {
      return Type.values()[value];
   }

   public static enum Type {
      /**
       * used in commit log garbage collector, in means that the minimum version in use should be returned
       */
      GET_VERSION,

      /**
       * used in view history garbage collector, it means to return the minimum view id in use
       */
      GET_VIEW_ID,

      /**
       * used in view history garbage collector, it means that all view id less than *this* view id can be safe deleted
       */
      SET_VIEW_ID
   }
}
