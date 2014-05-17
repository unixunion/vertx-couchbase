package com.scalabl3.vertxmods.couchbase.async;

import com.google.gson.Gson;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 5/11/13
 * Time: 4:08 PM
 * To change this template use File | Settings | File Templates.
 */


public class CouchbaseAsyncQuickTest extends Verticle {

    String address;
    @Override
    public void start() {
        System.out.println("\n\n\nCOUCHBASE Async!!!!\n\n\n");

        EventBus eb = vertx.eventBus();
        address = "vertx.couchbase.async";

        JsonObject config = new JsonObject();
        config.putString("address", address);
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "default");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.timeout.ms", 10000);
        config.putNumber("couchbase.tasks.check.ms", 500);
        config.putNumber("couchbase.num.clients", 1);

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.async.CouchbaseEventBusAsync", config, 1);
    }

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

        cbop.put("key", "op_incr");
        cbop.put("value", encode(11));

        act(cbop);

//        User u = new User("jasdeep", "jaitla");
//
//        cbop.put("key", "user1");
//        cbop.put("value", encode(u));

        act(cbop);
    }

    public void testGet() {
        final HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "get");
        cbop.put("key", "op_get");
        cbop.put("ack", true);


        pushHandle(compose(cbop), new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                System.out.println("received!: \n" + message.body().encode());

                JsonObject response = message.body().getObject("response");
                if (response.getBoolean("exists")) {
                    System.out.println(response.getString("key") + " = " + response.getObject("data").encode());
                } else {
                    System.out.println(response.getString("key") + " doesn't exist");
                }

            }
        });

        cbop.put("key", "op_incr");

        pushHandle(compose(cbop), new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                System.out.println("received!: \n" + message.body().encode());

                JsonObject response = message.body().getObject("response");
                if (response.getBoolean("exists")) {
                    System.out.println(response.getString("key") + " = " + response.getObject("data").encode());
                }
                else {
                    System.out.println(response.getString("key") + " doesn't exist");
                }

            }
        });

//        cbop.put("key", "user1");
//
//        pushHandle(compose(cbop), new Handler<Message<JsonObject>>() {
//            public void handle(Message<JsonObject> message) {
//                System.out.println("received!: \n" + message.body().encode());
//
//                JsonObject response = message.body().getObject("response");
//                if (response.getBoolean("exists")) {
//                    System.out.println(response.getString("key") + " = " + decode(response.getObject("data").getString("value"), User.class).toString());
//                }
//                else {
//                    System.out.println(response.getString("key") + " doesn't exist");
//                }
//
//            }
//        });

    }

    public void testIncrCreate() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "incr");
        cbop.put("key", "op_incr2");
        cbop.put("value", 1);
        cbop.put("default_value", 1);
        cbop.put("create", true);
        cbop.put("ack", true);

        act(cbop);
    }

    public void testIncrGet() {
        final HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "get");
        cbop.put("key", "op_incr2");
        cbop.put("ack", true);


        pushHandle(compose(cbop), new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                System.out.println("received!: \n" + message.body().encode());

                JsonObject response = message.body().getObject("response");
                if (response.getBoolean("exists")) {
                    System.out.println(response.getString("key") + " = " + response.getObject("data").encode());
                } else {
                    System.out.println(response.getString("key") + " doesn't exist");
                }

            }
        });
    }

    public void testIncr() {
        HashMap<String, Object> cbop = new HashMap<String, Object>();

        cbop.put("op", "incr");
        cbop.put("key", "op_incr2");
        cbop.put("ack", true);

        act(cbop);
    }

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

    private void push(JsonObject notif) {
        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                System.out.println("received: \n" + message.body().encode());
            }
        };
        vertx.eventBus().send(address, notif, replyHandler);
    }

    private void pushHandle(JsonObject msg, Handler<Message<JsonObject>> replyHandler) {
        vertx.eventBus().send(address, msg, replyHandler);
    }

}
