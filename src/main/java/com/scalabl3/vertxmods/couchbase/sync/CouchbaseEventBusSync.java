package com.scalabl3.vertxmods.couchbase.sync;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.CouchbaseClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;


/**
 * <p>Couchbase 2.x client for vert.x</p>
 * <p>In this module, all operations are using the synchronous variety of operations from Couchbase Java SDK.
 * This makes it better suited to be a Worker Verticle. Synchronous operations in Couchbase offer more parameter options
 * than the asynchronous varieties. However, under the hood, they use Netty/NIO so they are actually asynchronous as well.</p>
 *
 * <p>See the Async Couchbase verticle for pure async varieties of operations, you can use both in conjunction as separate Verticles
 * using different addresses.</p>
 *
 * Based partially on spymemcached client for vert.x by <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 *
 * @author <a href="mailto:jasdeep@scalabl3.com">Jasdeep Jaitla</a>
 *
 */
public class CouchbaseEventBusSync extends Verticle {
    private String eventbusAddress;
    private String cbnodes;
    private String bucket;
    private String bucketPassword;

    private EventBus eb;
    private Logger logger;
    private ArrayList<URI> couchbaseNodes;
    private CouchbaseClient[] couchbaseClients;

    @Override
    public void start() {
        logger = container.logger();
        eb = vertx.eventBus();

        // configurations
        eventbusAddress = container.config().getString("address", "vertx.couchbase.sync");
        cbnodes = container.config().getString("couchbase.nodelist", "localhost:8091");
        bucket = container.config().getString("couchbase.bucket", "ivault");
        bucketPassword = container.config().getString("couchbase.bucket.password", "");
        int connections = container.config().getNumber("couchbase.num.clients", 1).intValue();

        // init connection pool
        try {
            connectCouchbaseClients(connections);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // register verticle
        logger.info("registering at address: " + eventbusAddress);
        eb.registerHandler(eventbusAddress, cbSyncHandler, new AsyncResultHandler<Void>() {
            @Override
            public void handle(AsyncResult<Void> voidAsyncResult) {
                logger.info(this.getClass().getSimpleName() + " verticle up, listening on: " + eventbusAddress);
            }
        });
    }

    Handler<Message<JsonObject>> cbSyncHandler = new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> message) {
            logger.info("Got message:" + message.body().toString());
            String command = CouchbaseCommandPacketSync.voidNull(message.body().getString("op"));

            if (command.isEmpty()) {
                sendError(message, "\"op\" property is mandatory for request");
                return;
            }
            try {

                CouchbaseCommandPacketSync cbcps = getByName(command);
                JsonObject json = cbcps.operation(getCBClient(), message);

                if (requiresAcknowledgement(message)) {
                        acknowledge(message, cbcps.buildResponse(message, json, requiresAcknowledgement(message)));
                }

            } catch (TimeoutException e) {
                sendError(message, "operation '" + command + "' timed out");
            } catch (IllegalArgumentException e) {
                sendError(message, "unknown command: '" + command + "'");
            } catch (ExecutionException e) {
                sendError(message, "operation '" + command + "' failed");
            } catch (Exception e) {
                sendError(message, e.getMessage());
            }
        }
    };

    private boolean requiresAcknowledgement(Message<JsonObject> message) {
        return message.body().getBoolean("ack") != null && message.body().getBoolean("ack") == Boolean.TRUE;
    }

    private void acknowledge(Message<JsonObject> message, JsonObject response) {
        JsonObject reply = new JsonObject();
        reply.putObject("response", response);
        message.reply(reply);
    }

    private void sendError(Message<JsonObject> message, String errMsg) {
        JsonObject reply = new JsonObject();
        reply.putString("status", "error");
        reply.putString("message", errMsg);

        message.reply(reply);
    }

    private void connectCouchbaseClients(int connections) throws IOException {
        couchbaseClients = new CouchbaseClient[connections < 1 ? 1 : connections];
        for (int i = 0; i < couchbaseClients.length; i++) {
            couchbaseClients[i] = new CouchbaseClient(ParseNodeList.getAddresses(cbnodes), bucket, bucketPassword);
        }
    }

    private CouchbaseClient getCBClient() {
        if (couchbaseClients.length == 1)
            return couchbaseClients[0];
        return couchbaseClients[(int) (Math.random() * couchbaseClients.length)];
    }

    private CouchbaseCommandPacketSync getByName(String name) {
        if (name == null) {
            return null;
        }
        return CouchbaseCommandPacketSync.valueOf(name.toUpperCase());
    }
}
