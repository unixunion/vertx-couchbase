package com.deblox.couchperftest;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Created by keghol on 5/26/14.
 */
public class SimpleReadTest extends Verticle implements Handler<Message<JsonObject>> {

    private long start;
    private int count = 0;
    private JsonObject config;
    private static final int CREDITS_BATCH = 2000;  // This determines the degree of pipelining
    private int requestCredits = CREDITS_BATCH;
    private EventBus eb;

    public void start() {
        System.out.println("Starting Simple Read Test");
        config = Util.loadConfig(this, "/conf-perftest.json");
        eb = vertx.eventBus();
        makeRequest();
    }

    public void handle(Message<JsonObject> response) {

        if (Util.getSuccess(response)) {
            count++;
        } else {
            System.err.println("error " + response.body());
        }

        if (count % 100 == 0) {
            eb.send("rate-counter", count);
            count = 0;
        }
        requestCredits++;
        makeRequest();
    }

    private void makeRequest() {

        if (start == 0) {
            start = System.currentTimeMillis();
        }

        while (requestCredits > 0) {

            JsonObject request = new JsonObject()
                    .putString("op", "get")
                    .putString("key", "user" + (int) (10000 * Math.random()))
                    .putBoolean("ack", true);

            eb.send(config.getString("address"), request, this);

            requestCredits--;
        }
    }

}
