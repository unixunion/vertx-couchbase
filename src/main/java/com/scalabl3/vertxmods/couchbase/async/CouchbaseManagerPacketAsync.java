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

     Request
    {
      "management": "CREATEBUCKET",
      "name": "test",
      "bucketType": "couchbase",
      "memorySizeMB": 128,
      "replicas": 0,
      "authPassword": "",
      "flushEnabled": true,
      "ack": true
    }

    Response
    {
        "response": {
            "success": true
        }
    }

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

    /*
     Create Port Bucket

     Request
    {
        "management": "CREATEPORTBUCKET",
        "name": "test_port",
        "bucketType": "couchbase", // couchbase OR membase
        "memorySizeMB": 128,
        "replicas": 0,
        "port": 30000,
        "flushEnabled": true,
        "ack": true
    }

     Response
    {
        "response": {
            "success": true
        }
    }
      */
    CREATEPORTBUCKET() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            BucketType bucketType = Enum.valueOf(BucketType.class, message.body().getString("bucketType").toUpperCase());
            Integer memorySizeMB = message.body().getInteger("memorySizeMB");
            Integer replicas = message.body().getInteger("replicas");
            Integer port = message.body().getInteger("port");
            Boolean flushEnabled = message.body().getBoolean("flushEnabled");

            Future<Boolean> f = new CompletedFuture(true);

            try {
                cm.createPortBucket(bucketType, name, memorySizeMB, replicas, port, flushEnabled);
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

    /*
    Delete bucket

    Request
    {
      "management": "DELETEBUCKET",
      "name": "test_port",
      "ack": true
    }

    Response
    {
        "response": {
            "success": true
        }
    }

     */

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

    /*
    Flush the buckets documents. This deletes ALL documents in the bucket!

    Request
    {
      "management": "FLUSHBUCKET",
      "name": "test",
      "ack": true
    }

    Response
    {
        "response": {
            "success": true
        }
    }

     */

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

    /*
    LISTBUCKETS

    returns a list of buckets from the couchcluster

    Request
    {
        "management":"LISTBUCKETS",
        "ack":true
    }

    Response
    {
      "response": {
        "data": [
          "async",
          "default",
          "sync"
        ],
        "success": true
      }
    }
     */
    LISTBUCKETS() {

        @Override
        public Future operation(ClusterManager cm, Message<JsonObject> message) throws Exception {

            JsonObject response = new JsonObject();

            try {
                ArrayList l = (ArrayList)cm.listBuckets();
                response.putArray("data", new JsonArray(l));
                response.putBoolean("success", true);
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

    public abstract Future operation(ClusterManager cb, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, Future result, boolean returnAcknowledgement) throws Exception;



}
