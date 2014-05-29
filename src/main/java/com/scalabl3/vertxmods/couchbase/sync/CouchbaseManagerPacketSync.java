package com.scalabl3.vertxmods.couchbase.sync;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;

/**
 * Created by keghol on 5/29/14.
 */

public enum CouchbaseManagerPacketSync {

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
        public JsonObject operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            BucketType bucketType = Enum.valueOf(BucketType.class, message.body().getString("bucketType").toUpperCase());
            Integer memorySizeMB = message.body().getInteger("memorySizeMB");
            Integer replicas = message.body().getInteger("replicas");
            String authPassword = message.body().getString("authPassword");
            Boolean flushEnabled = message.body().getBoolean("flushEnabled");

            JsonObject response = new JsonObject();

            try {
                cm.createNamedBucket(bucketType, name, memorySizeMB, replicas, authPassword, flushEnabled);
                response.putBoolean("success", true);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            return response;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            return result;
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
        public JsonObject operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            BucketType bucketType = Enum.valueOf(BucketType.class, message.body().getString("bucketType").toUpperCase());
            Integer memorySizeMB = message.body().getInteger("memorySizeMB");
            Integer replicas = message.body().getInteger("replicas");
            Integer port = message.body().getInteger("port");
            Boolean flushEnabled = message.body().getBoolean("flushEnabled");

            JsonObject response = new JsonObject();

            try {
                cm.createPortBucket(bucketType, name, memorySizeMB, replicas, port, flushEnabled);
                response.putBoolean("success", true);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            return response;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            return result;
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
        public JsonObject operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            JsonObject response = new JsonObject();

            try {
                cm.deleteBucket(name);
                response.putBoolean("success", true);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            return response;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            return result;
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
        public JsonObject operation(ClusterManager cm, Message<JsonObject> message) throws Exception {
            String name = message.body().getString("name");

            JsonObject response = new JsonObject();

            try {
                cm.flushBucket(name);
                response.putBoolean("success", true);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            return response;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

            return result;
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
        public JsonObject operation(ClusterManager cm, Message<JsonObject> message) throws Exception {

            JsonObject response = new JsonObject();

            try {
                ArrayList l = (ArrayList)cm.listBuckets();
                response.putArray("data", new JsonArray(l));
                response.putBoolean("success", true);
            } catch (Exception e) {
                e.printStackTrace();
                response.putBoolean("success", false);
            }

            return response;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception {

            if(!returnAcknowledgement) {
                return null;
            }

           return result;
        }
    },


    ;

    public abstract JsonObject operation(ClusterManager cb, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, JsonObject result, boolean returnAcknowledgement) throws Exception;


}
