package com.scalabl3.vertxmods.couchbase.test;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.util.HashMap;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class Main extends TestVerticle {

    String address;
    JsonObject config;

    // timers  and counters
    long startTime;
    long endTime;
    long timeEnded;
    Integer count = 0;
    Integer count_max = 1;

    // used to count async results and finalize tests
    public void count() {
        count=count+1;
        if (count > count_max-1) {
            endTime = System.currentTimeMillis();
            timeEnded =  ((endTime-startTime) /1000);
            System.out.println("rate achieved: " + (count_max+1/(timeEnded+1)) + " msgs/ps");
            count_max=1;
            count=0;
            testComplete();
        }
    }

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();

        config.putString("address", "vertx.couchbase.sync");
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "default");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);
        config.putBoolean("async_mode", false);


        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Sync\n\n");

        container.deployWorkerVerticle("com.scalabl3.vertxmods.couchbase.Boot", config, 1, true, new AsyncResultHandler<String>() {

             @Override
            public void handle(AsyncResult<String> asyncResult) {

                // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause());
                }

                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());
                 try {
                     Thread.sleep(1000);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
                 // If deployed correctly then start the tests!
                startTests();
            }
        });


    }

    // Simple method to add a User object, id is appended to Username
    public void add(Integer id) {

        // Create a new user object via User class
        User user = new User("user"+id, "somepassword");
        String user_string = Util.encode(user);
//        System.out.println("user_string: " + user_string);

        JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", id.toString())
                .putString("value", user_string)
                .putNumber("expiry", 86400)
                .putBoolean("ack", true);

        container.logger().debug("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject body = reply.body();
                    assertNotNull(body.toString());
                    count();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("shit happens");
                }
            }
        });
    }
//
//    @Test
//    public void addBenchmark() {
//        startTime = System.currentTimeMillis();
//        endTime = 0;
//
//        // set the count_max before the test
//        count_max = 10000;
//        for(int i=0; i < count_max; i++) {
//            add(i);
//        }
//    }


    @Test
    public void get() {
        JsonObject request = new JsonObject().putString("op", "GET")
                .putString("key", "user1")
                .putBoolean("ack", true);

        container.logger().info("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    System.out.println("Response: " + reply.body());
                    JsonObject body = reply.body();
                    assertTrue(body.getObject("response").getBoolean("exists"));
                    testComplete();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
    }



    public void query_key(Integer id) {

         JsonObject request = new JsonObject().putString("op", "QUERY")
                .putString("design_doc", "users")
                .putString("view_name", "users")
                .putString("key", "user" + id)
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        container.logger().debug("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
//                    System.out.println("Response: " + reply.body());
                    JsonObject body = reply.body();

//                    assertNotNull(body.toString());
                    testComplete();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
    }


    @Test
    public void query_single_key() {
        add(99999);
        query_key(99999);
    }

    @Test
    public void query_keys() {
        add(99991);
        add(99992);

        JsonObject request = new JsonObject().putString("op", "QUERY")
                .putString("design_doc", "users")
                .putString("view_name", "users")
                .putArray("keys", new JsonArray()
                        .addString("user99991")
                        .addString("user99992"))
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        container.logger().debug("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    System.out.println("Response: " + reply.body());
                    JsonObject body = reply.body();
                    assertNotNull(body.toString());
                    testComplete();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
    }


    public void act(HashMap<String, Object> cmd)
    {
        if(cmd == null)
            return;

        JsonObject notif = new JsonObject();

        for(String key : cmd.keySet())
        {
            Object value = cmd.get(key);

            if(value != null)
            {
                if(value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if(value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if(value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if(value instanceof String)
                    notif.putString(key, (String) value);
                else if(value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        System.out.println("sent: \n" + notif.encode());
        push(notif);
    }

    private void push(JsonObject notif)
    {
        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                System.out.println("received: \n" +message.body().encode());
            }
        };
        vertx.eventBus().send(address, notif, replyHandler);
    }
}
