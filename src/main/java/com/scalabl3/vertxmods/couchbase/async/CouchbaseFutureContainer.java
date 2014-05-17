package com.scalabl3.vertxmods.couchbase.async;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.Future;

public class CouchbaseFutureContainer
{
   private long startTime;
   private Future future;
   private Message<JsonObject> message;
   private CouchbaseCommandPacketAsync cbcp;

   public CouchbaseFutureContainer(Future future, Message<JsonObject> message, CouchbaseCommandPacketAsync cbcp)
   {
      this.future = future;
      this.message = message;
      this.cbcp = cbcp;
      this.startTime = System.currentTimeMillis();
   }

   public Future getFuture()
   {
      return future;
   }

   public long getStartTime()
   {
      return startTime;
   }

   public Message<JsonObject> getMessage()
   {
      return message;
   }

   public CouchbaseCommandPacketAsync getCommand()
   {
      return cbcp;
   }
}
