package com.scalabl3.vertxmods.couchbase.test;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class CouchbaseAsyncTests extends TestVerticle{

    JsonObject config;

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();

        config.putString("address", "vertx.couchbase.async");
        config.putString("couchbase.nodelist", "localhost:8091");
        config.putString("couchbase.bucket", "ivault");
        config.putString("couchbase.bucket.password", "");
        config.putNumber("couchbase.num.clients", 1);
        config.putBoolean("async_mode", true);

        System.out.println("\n\n\nDeploy Worker Verticle Couchbase Async\n\n");


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

        JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", id.toString())
                .putString("value", user_string)
                .putNumber("expiry", 86400)
                .putBoolean("ack", true);

        container.logger().info("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    container.logger().info("Add response: " + reply.body().toString());
                    JsonObject body = reply.body();
                    assertNotNull(body.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("shit happens");
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
                    System.out.println("Response: " + reply.body());
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


//    @Test
//    public void query_single_key() {
//        add(100001);
//        query_key(1000001);
//    }

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
}
