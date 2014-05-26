package com.scalabl3.vertxmods.couchbase.async;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;
import com.scalabl3.vertxmods.couchbase.CompletedFuture;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;


/**
 * Created by keghol on 5/25/14.
 */

@SuppressWarnings("unchecked")
public enum CouchbaseManagerPacketAsync {
    /*
     Create Bucket
      */
    CREATEBUCKET() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            BucketType bucketType = Enum.valueOf(BucketType.class, message.body().getString("bucketType").toUpperCase());
            Integer memorySizeMB = message.body().getInteger("memorySizeMB");
            Integer replicas = message.body().getInteger("replicas");
            String authPassword = message.body().getString("authPassword");
            Boolean flushEnabled = message.body().getBoolean("flushEnabled");

            Future<Boolean> f = new CompletedFuture(true);

            try {
                cm.createNamedBucket(bucketType, name, memorySizeMB, replicas, authPassword, flushEnabled);
            } catch (Exception e) {
                e.printStackTrace();
                f = new CompletedFuture(false);
            }

            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = new JsonObject();
            response.putBoolean("success", (Boolean)result.get());

            return response;
        }
    },

    DELETEBUCKET() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            Future<Boolean> f = new CompletedFuture(true);

            try {
                cm.deleteBucket(name);
            } catch (Exception e) {
                e.printStackTrace();
                f = new CompletedFuture(false);
            }

            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = new JsonObject();
            response.putBoolean("success", (Boolean)result.get());

            return response;
        }
    },

    FLUSHBUCKET() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            Future<Boolean> f = new CompletedFuture(true);

            try {
                cm.flushBucket(name);
            } catch (Exception e) {
                e.printStackTrace();
                f = new CompletedFuture(false);
            }

            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject response = new JsonObject();
            response.putBoolean("success", (Boolean)result.get());

            return response;
        }
    },

    LISTBUCKETS() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
//            String name = message.body().getString("name");


            JsonObject response = new JsonObject();

            try {
//                response.p
                ArrayList l = (ArrayList)cm.listBuckets();
                response.putArray("data", new JsonArray(l));
                response.putBoolean("success", true);
//                f = new CompletedFuture(response);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            Future<List> f = new CompletedFuture(response);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            JsonObject data = (JsonObject)result.get();
            return data;
        }
    },


    ;

//    public abstract JsonObject operation(ClusterManager cb, Message<JsonObject> message, Boolean async) throws Exception;
//
//    public abstract JsonObject buildResponse(Message<JsonObject> message, Future future, boolean returnAcknowledgement) throws Exception;

    public abstract Future operation(ClusterManager cb, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception;



}
