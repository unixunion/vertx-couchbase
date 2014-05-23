package com.scalabl3.vertxmods.couchbase.test;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.vertx.testtools.VertxAssert.*;

public class ClusterManagerTests extends TestVerticle {

    EventBus eb;
    JsonObject config;
    DefaultPrettyPrinter pp;
    ObjectMapper mapper;

    @Override
    public void start() {
        initialize();

        eb = vertx.eventBus();
        config = loadConfig("/conf.json");

        // prettyprinter
        pp = new DefaultPrettyPrinter();
        pp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());
        mapper = new ObjectMapper();

        System.out.println("\n\n\nDeploying Mod Couchbase\n\n");

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

    @Test
    public void getBuckets() {
        JsonObject request = new JsonObject();
        request.putString("name", "all");

        String address = config.getString("address") + ".mgmt.bucket";
        container.logger().info("sending to address: " + address);

        eb.send(address, request, new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> event) {
                container.logger().info("test_response");

                container.logger().info("response: " + event.body());

                try {
                    System.out.println("Pretty: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event.body().toString()));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                testComplete();
            };
        });

    }


    JsonObject loadConfig(String file) {
//        System.out.println(System.getProperty("java.class.path"));
        try (InputStream stream = this.getClass().getResourceAsStream(file)) {
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));

            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append('\n');
                line = reader.readLine();
                System.out.println(line);
            }

            return new JsonObject(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
            return new JsonObject();
        }

    }

}
