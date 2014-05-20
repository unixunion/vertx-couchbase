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

import com.scalabl3.vertxmods.couchbase.test.Util;
import com.scalabl3.vertxmods.couchbase.test.User;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Created by marzubus on 18/05/14.
 */

public class QueryTests extends TestVerticle {

    JsonObject config;

    // timers
    long startTime;
    long endTime;
    long timeEnded;
    Integer count = 0;
    Integer count_max = 1;

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();

        config.putString("address", "vertx.couchbase.sync");
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "ivault");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);
        config.putBoolean("async_mods", false);

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

    // used to count async results and finalize tests
    public void count() {
        count=count+1;
        if (count > count_max-1) {
            endTime = System.currentTimeMillis();
            timeEnded =  ((endTime-startTime) /1000);
            System.out.println("rate achieved: " + (count_max/timeEnded) + " msgs/ps");
            count_max=1;
            count=0;
            testComplete();
        }
    }


    public void query_key(Integer i) {
            JsonObject request = new JsonObject().putString("op", "QUERY")
                    .putString("design_doc", "users")
                    .putString("view_name", "users")
                    .putString("key", "user" + i)
                    .putBoolean("include_docs", true)
                    .putBoolean("ack", true);

            container.logger().debug("sending message to address: " + config.getString("address"));

            vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

                @Override
                public void handle(final Message<JsonObject> reply) {
                    try {
                        // convert the 1st object in the respone.respons.result section of the reply message
//                        System.out.println("reply was: " + reply.body());
                        User u = (User)Util.decode(reply.body()
                                .getObject("response")
                                .getObject("response")
                                .getArray("result").get(0)
                                .toString(), User.class );
                        if ( u.getPassword().equals("somepassword")) {
                            count();
                        } else {
                            System.out.println("Error, password missmatch, check your data: " + u.getPassword() + " : " + u.toString());
                            System.out.println("reply was: " + reply.body());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            });
    }

    @Test
    public void get_keys() {
        JsonObject request = new JsonObject().putString("op", "QUERY")
                .putString("design_doc", "users")
                .putString("view_name", "users")
                .putString("key", "[\"user0\",\"somepassword\"]")
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Try to deserialize reply: " + reply.body().toString());

                try {
                    User u = (User)Util.decode(reply.body()
                            .getObject("response")
                            .getObject("response")
                            .getArray("result").get(0)
                            .toString(), User.class );
                    if ( u.getPassword().equals("somepassword")) {
                        testComplete();
                    } else {
                        System.out.println("Error, password missmatch, check your data: " + u.getPassword() + " : " + u.toString());
                        System.out.println("reply was: " + reply.body());
                        fail();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                    throw e;

                }
            }
        });
    }

    @Test
    public void get_missing_keys() {
        JsonObject request = new JsonObject().putString("op", "QUERY")
                .putString("design_doc", "users")
                .putString("view_name", "users")
                .putString("key", "[\"user0\",\"nopass\"]")
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Reply: " + reply.body().toString());

                try {
//                    String r = reply.body().getObject("response").getObject("response").getArray("result").toString();
                    assertEquals("[]", reply.body()
                            .getObject("response")
                            .getObject("response")
                            .getArray("result")
                            .toString());
                    testComplete();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                    throw e;
                }
            }
        });
    }


//    @Test
//    public void get_keys_nodocs() {
//        JsonObject request = new JsonObject().putString("op", "QUERY")
//                .putString("design_doc", "users")
//                .putString("view_name", "users")
//                .putString("key", "[\"user0\",\"somepassword\"]")
//                .putBoolean("include_docs", false)
//                .putBoolean("ack", true);
//
//        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
//
//            @Override
//            public void handle(final Message<JsonObject> reply) {
//                System.out.println("Try to deserialize reply: " + reply.body().toString());
//
//                try {
//                       String f = reply.body()
//                            .getObject("response")
//                            .getObject("response")
//                            .getArray("result").get(0).toString();
//
//                       assertEquals("user0", f);
//                        testComplete();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    fail();
//                    throw e;
//
//                }
//            }
//        });
//    }

//    @Test
//    public void keyBenchmark() {
//        startTime = System.currentTimeMillis();
//        endTime = 0;
//
//        count_max=10000;
//        System.out.println("firing off queries");
//        for(int i=0; i < count_max; i++) {
//            query_key(i);
//        }
//        System.out.println("done, waiting for async");
//    }

}
