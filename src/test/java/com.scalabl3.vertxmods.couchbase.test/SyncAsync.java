package com.scalabl3.vertxmods.couchbase.test;

import com.google.gson.Gson;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.vertx.testtools.VertxAssert.*;

/**
 Compare async vs sync results and structures
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SyncAsync extends TestVerticle{

    JsonObject async_config;
    JsonObject sync_config;

    // timers
    long startTime;
    long endTime;
    long timeEnded;
    Integer count = 1;
    Integer count_max = 1;

    // other
    Integer post_count = 0;
    Integer post_max = 2;
    //Message[] posts = new Message[0];
    List<Message> posts = new ArrayList<Message>();

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();

        async_config = new JsonObject();
        async_config.putString("address", "vertx.couchbase.async");
        async_config.putString("couchbase.nodelist", "localhost:8091");
        async_config.putString("couchbase.bucket", "default");
        async_config.putString("couchbase.bucket.password", "");
        async_config.putNumber("couchbase.num.clients", 1);
        async_config.putBoolean("async_mode", true);

        sync_config = new JsonObject();
        sync_config.putString("address", "vertx.couchbase.sync");
        sync_config.putString("couchbase.nodelist", "localhost:8091");
        sync_config.putString("couchbase.bucket", "default");
        sync_config.putString("couchbase.bucket.password", "");
        sync_config.putNumber("couchbase.num.clients", 1);
        sync_config.putBoolean("async_mode", false);


        System.out.println("\n\nDeploy Worker Verticle Couchbase\n\n");

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.Boot", async_config, 1, true, new AsyncResultHandler<String>() {

            @Override
            public void handle(AsyncResult<String> asyncResult) {

                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());

                container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.Boot", sync_config, 1, true, new AsyncResultHandler<String>() {

                    @Override
                    public void handle(AsyncResult<String> asyncResult) {

                        assertTrue(asyncResult.succeeded());
                        assertNotNull("deploymentID should not be null", asyncResult.result());

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        startTests();
                    }
                });

            }
        });


    }



    @Test
    public void testSet() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        ArrayList<String> x = new ArrayList<String>();
        x.add("couchbase");
        x.add("nuodb");

        cbop.put("op", "set");
        cbop.put("key", "op_get");
        cbop.put("value", encode(x));
        cbop.put("ack", true);
        act(cbop);

//        cbop.put("key", "op_incr");
//        cbop.put("value", encode(11));
//        act(cbop);
    }


    // create incr / decr dependency docs
    @Test
    public void aDecrIncr_prepare() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "set");
        cbop.put("ack", true);
        cbop.put("key", "op_incr");
        cbop.put("value", encode(11));
        act(cbop);
    }

    @Test
    public void testIncr() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "incr");
        cbop.put("ack", true);
        cbop.put("key", "op_incr");
        cbop.put("by", 11);
        act(cbop);
    }


    @Test
    public void testDecr() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "decr");
        cbop.put("ack", true);
        cbop.put("key", "op_incr");
        cbop.put("by", 11);
        act(cbop);
    }



//    @Test
//    public void get_keys() {
//
//        post_count = 0;
//        add("user" + 1001);
//
//        JsonObject request = new JsonObject().putString("op", "GET")
//                .putString("key", "user1001")
//                .putBoolean("include_docs", true)
//                .putBoolean("ack", true);
//
//        vertx.eventBus().send(sync_config.getString("address"), request, new Handler<Message<JsonObject>>() {
//
//            @Override
//            public void handle(final Message<JsonObject> reply) {
//                System.out.println("sync_reply: " + reply.body().toString());
//                try {
//                    post(reply.body().toString());
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    fail();
//                    throw e;
//
//                }
//            }
//        });
//
//
//        vertx.eventBus().send(async_config.getString("address"), request, new Handler<Message<JsonObject>>() {
//
//            @Override
//            public void handle(final Message<JsonObject> reply) {
//                System.out.println("async_reply: " + reply.body().toString());
//                try {
//                    post(reply.body().toString());
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    fail();
//                    throw e;
//
//                }
//            }
//        });
//
//
//    }








    public JsonObject compose(HashMap<String, Object> cmd) {
        if (cmd == null)
            return null;

        JsonObject notif = new JsonObject();

        for (String key : cmd.keySet()) {
            Object value = cmd.get(key);

            if (value != null) {
                if (value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if (value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if (value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if (value instanceof String)
                    notif.putString(key, (String) value);
                else if (value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        return notif;
    }

    public void act(HashMap<String, Object> cmd) {
        if (cmd == null)
            return;

        JsonObject notif = new JsonObject();

        for (String key : cmd.keySet()) {
            Object value = cmd.get(key);

            if (value != null) {
                if (value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if (value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if (value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if (value instanceof String)
                    notif.putString(key, (String) value);
                else if (value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        System.out.println("sent: \n" + notif.encode());
        push(notif);
    }

    private String encode(Object val) {
        Gson gson = new Gson();
        return gson.toJson(val);
    }

    private Object decode(String val, Class<?> typeOfT) {
        Gson gson = new Gson();
        return gson.fromJson(val, typeOfT);
    }

    private Object decode(String val, Type typeOfT) {
        Gson gson = new Gson();
        return gson.fromJson(val, typeOfT);
    }

    private void compare(Message m1, Message m2) {
        System.out.println("comparing messages in " + m1.body() + " and " + m2.body());

        JsonObject j1 = new JsonObject(m1.body().toString()).getObject("response");
        JsonObject j2 = new JsonObject(m2.body().toString()).getObject("response");

        assertEquals(j1.getBoolean("success"), j2.getBoolean("success"));
        assertEquals(j1.getString("op"), j2.getString("op"));
        assertEquals(j1.getString("key"), j2.getString("key"));

        testComplete();
        posts.clear();

    }


    // called with result of async / sync call, when count hits two, compare em!
    public void post(Message result) {

        System.out.println("Result: " + result.body());

        post_count=post_count+1;
//        posts[post_count] = result;
        posts.add(result);

        if (post_count >=2) {
            post_count = 0;
            compare(posts.get(0), posts.get(1));
        }
    }


    // send both async and sync events
    private void push(JsonObject notif) {
        push(notif, true);
        push(notif, false);
    }

    private void push(JsonObject notif, Boolean async) {

        if (async) {

            Handler<Message<JsonObject>> async_replyHandler = new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> message) {
                    System.out.println("async received: \n" + message.body().encode());
                    post(message);
                }
            };
            vertx.eventBus().send(async_config.getString("address"), notif, async_replyHandler);

        } else {

            Handler<Message<JsonObject>> sync_replyHandler = new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> message) {
                    System.out.println("sync received: \n" + message.body().encode());
                    post(message);
                }
            };
            vertx.eventBus().send(sync_config.getString("address"), notif, sync_replyHandler);

        }
    }

    private void pushHandle(JsonObject msg, Handler<Message<JsonObject>> replyHandler) {
        vertx.eventBus().send(async_config.getString("address"), msg, replyHandler);
        vertx.eventBus().send(sync_config.getString("address"), msg, replyHandler);
    }





}
