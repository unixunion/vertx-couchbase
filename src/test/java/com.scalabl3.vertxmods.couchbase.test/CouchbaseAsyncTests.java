package com.scalabl3.vertxmods.couchbase.test;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import com.scalabl3.vertxmods.couchbase.test.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Created with IntelliJ IDEA.
 * User: jasdeep
 * Date: 6/8/13
 * Time: 9:00 AM
 * To change this template use File | Settings | File Templates.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CouchbaseAsyncTests extends TestVerticle{

    JsonObject config;

    // timers
    long startTime;
    long endTime;
    long timeEnded;
    Integer count = 1;
    Integer count_max = 1;

    @Override
    public void start() {
        initialize();

        EventBus eb = vertx.eventBus();
        config = new JsonObject();
        config = Util.loadConfig(this, "/conf-async.json");

        System.out.println("\n\n\nDeploy mod-couchbase\n\n");

        container.deployVerticle("com.scalabl3.vertxmods.couchbase.Boot", config, new AsyncResultHandler<String>() {

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


    // Simple method to add a User object, id is appended to Username
    public void add(String username, String password) {

//        String hashed = "somepassword";

        User user = new User(username, password);
        String user_string = Util.encode(user);

        JsonObject request = new JsonObject().putString("op", "ADD")
                .putString("key", user.getUsername())
                .putString("value", user_string)
                .putNumber("expiry", 86400)
                .putBoolean("ack", true);

        container.logger().debug("sending message to address: " + config.getString("address"));

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    container.logger().debug("Add response: " + reply.body().toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("shit happens");
                }
            }
        });
    }

    public void query_key(String username, final Handler<Message<JsonObject>> callback) {

        JsonObject request = new JsonObject().putString("op", "QUERY")
                .putString("design_doc", "users")
                .putString("view_name", "users")
                .putString("key", username)
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        container.logger().debug("sending message to address: " + config.getString("address"));


        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    container.logger().debug("Response: " + reply.body());
                    callback.handle(reply);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
    }

    @Test
    public void query_single_key() {

        // create the document
        add("user" + 100001, "somepassword");

        // install the view
        create_design_document(new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                // query the view for the key
                System.out.println("output received " + reply.body());
                query_key("user" + 100001, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> reply) {
                        System.out.println("output received " + reply.body());
                        testComplete();
                    }
                });
            }
        });



    }

    @Test
    public void set() {

        JsonObject request = new JsonObject().putString("op", "SET")
                .putString("key", "user1001")
                .putString("value", "data")
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Try to deserialize reply: " + reply.body().toString());
                testComplete();
            }
        });
    }


    @Test
    public void add_keys() {
        count_max = 15000;
        startTime = System.currentTimeMillis();

        for(int i=0; i < count_max; i++) {
            add("user" + i, "somepassword");
            count();
        }
    }


    public void create_design_document(final Handler<Message<JsonObject>> callback) {

        ViewDesign view1 = new ViewDesign(
                "users",
                "function (doc, meta) {\n" +
                        "  emit(doc.username, doc.password);\n" +
                        "}"
        );

        DesignDocument dd = new DesignDocument("dev_users");
        dd.setView(view1);

        JsonObject request = new JsonObject().putString("op", "CREATEDESIGNDOC")
                .putString("name", "users")
                .putString("value", dd.toJson())
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Got Response : " + reply.body());
                assertEquals(true, Util.getResponse(reply).getBoolean("success"));
                callback.handle(reply);
//                testComplete();
            }
        });

    }


    @Test
    public void create_design_document_error() {

        JsonObject request = new JsonObject().putString("op", "CREATEDESIGNDOC")
                .putString("name", "dev_test1")
                .putString("value", "error rorororro")
                .putBoolean("ack", true);

        System.out.println(request.toString());

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                assertEquals(false, Util.getResponse(reply).getBoolean("success"));
                System.out.println("Got Response : " + reply.body());
                testComplete();
            }
        });

    }


    @Test
    public void get_xdelete_document() {

        create_design_document(new Handler<Message<JsonObject>>() {
                                   @Override
                                   public void handle(Message<JsonObject> reply) {
                                       JsonObject request = new JsonObject().putString("op", "DELETEDESIGNDOC")
                                               .putString("name", "dev_test")
                                               .putBoolean("ack", true);

                                       vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

                                           @Override
                                           public void handle(final Message<JsonObject> reply) {
                                               assertEquals(true, Util.getResponse(reply).getBoolean("success"));
                                               testComplete();
                                                                           }
                                       });
                                   }
       });

    }


    @Test
    public void get_design_document() {

        create_design_document(new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject request = new JsonObject().putString("op", "GETDESIGNDOC")
                        .putString("name", "users")
                        .putBoolean("ack", true);

                vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> reply) {
                        System.out.println("Got response: " + reply.body());
                        assertEquals(true, Util.getResponse(reply).getBoolean("exists"));
                        testComplete();
                    }
                });
            }
        });


    }

    @Test
    public void get_missing_design_document() {
        JsonObject request = new JsonObject().putString("op", "GETDESIGNDOC")
                .putString("design_doc", "dev_testdsds")
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Got response: " + reply.body());
                assertEquals("error", reply.body().getString("status"));
                testComplete();
            }
        });
    }

    @Test
    public void get_keys() {

        add("user" + 1001, "somepassword");

        JsonObject request = new JsonObject().putString("op", "GET")
                .putString("key", "user1001")
                .putBoolean("include_docs", true)
                .putBoolean("ack", true);

        vertx.eventBus().send(config.getString("address"), request, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(final Message<JsonObject> reply) {
                System.out.println("Try to deserialize reply: " + reply.body().toString());

                try {
                    String user = reply.body()
                            .getObject("response")
                            .getObject("data")
                            .getString("value");
                    User u = (User)Util.decode(user, User.class );

                    System.out.println("UserObject password: " + u.getPassword());

                    if ("somepassword".equals("somepassword")){
                        testComplete();
                    } else {
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
