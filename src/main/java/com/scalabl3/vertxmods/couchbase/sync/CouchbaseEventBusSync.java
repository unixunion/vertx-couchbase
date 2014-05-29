package com.scalabl3.vertxmods.couchbase.sync;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.CouchbaseClient;
import com.scalabl3.vertxmods.couchbase.ParseNodeList;
import com.scalabl3.vertxmods.couchbase.Util;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * Couchbase 2.x client for vert.x<p>
 * Please see the manual for a full description<p>
 *
 * This is a fork of https://github.com/scalabl3/vertx-couchbase
 *
 * Based partially on spymemcached client for vert.x by <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 * @author <a href="mailto:jasdeep@scalabl3.com">Jasdeep Jaitla</a>
 * @author <a href="mailto:marzubus@gmail.com">Kegan Holtzhausen</a>
 *
 */

public class CouchbaseEventBusSync extends Verticle {
    private String eventbusAddress;
    private String cbnodes;
    private String manager_username;
    private String manager_password;
    private Boolean manager_connect;
    private String bucket;
    private String bucketPassword;

    private EventBus eb;
    private Logger logger;
    private ArrayList<URI> couchbaseNodes;
    private CouchbaseClient[] couchbaseClients;
    private ClusterManager clusterManager;

    @Override
    public void start() {
        logger = container.logger();
        eb = vertx.eventBus();

        // configurations
        eventbusAddress = container.config().getString("address", "vertx.couchbase.sync");
        cbnodes = container.config().getString("couchbase.nodelist", "localhost:8091");
        manager_username = container.config().getString("couchbase.manager.username");
        manager_password = container.config().getString("couchbase.manager.password");
        manager_connect = container.config().getBoolean("couchbase.manager", false);
        bucket = container.config().getString("couchbase.bucket", "ivault");
        bucketPassword = container.config().getString("couchbase.bucket.password", "");
        int connections = container.config().getNumber("couchbase.num.clients", 1).intValue();

        // init connection pool
        try {
            connectCouchbaseClients(connections);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // init manager connection
        if (manager_connect) {
            try {
                connectClusterManager();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
            logger.debug("Got message:" + message.body().toString());
            String command = Util.voidNull(message.body().getString("op"));
            String management_command = Util.voidNull(message.body().getString("management"));

            if (command.isEmpty() && management_command.isEmpty()) {
                sendError(message, "\"op\" OR \"management\" property is mandatory for request");
                return;
            }

            if (!command.isEmpty() && !management_command.isEmpty()) {
                sendError(message, "cannot perform OP and MANAGEMENT at the same time, choose one!");
                return;
            }

            try {

                CouchbaseCommandPacketSync cbcps = null;
                CouchbaseManagerPacketSync cbmps = null;
                JsonObject json = null;


                if (!command.isEmpty()) {
                    cbcps = getByName(command);
                    json = cbcps.operation(getCBClient(), message);
                } else if (!management_command.isEmpty()) {
                    cbmps = getMgmtByName(management_command);
                    json = cbmps.operation(getCMClient(), message);
                } else {
                    sendError(message, "unable to route command to a client / manager");
                }

                if (requiresAcknowledgement(message)) {
                    if (!command.isEmpty()) {
                        acknowledge(message, cbcps.buildResponse(message, json, requiresAcknowledgement(message)));
                    } else if (!management_command.isEmpty()) {
                        acknowledge(message, cbmps.buildResponse(message, json, requiresAcknowledgement(message)));
                    }
                }

            } catch (TimeoutException e) {
                sendError(message, "operation '" + command + "' timed out");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                sendError(message, "unknown command: '" + command + "'");
            } catch (ExecutionException e) {
                sendError(message, "operation '" + command + "' failed");
            } catch (Exception e) {
                e.printStackTrace();
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

    private void connectClusterManager() throws IOException {
        container.logger().info("Connecting ClusterManager");
        clusterManager = new ClusterManager(ParseNodeList.getAddresses(cbnodes), manager_username, manager_password);
    }

    private ClusterManager getCMClient() {
        return clusterManager;
    }


    private CouchbaseManagerPacketSync getMgmtByName(String name) {
        if (name == null) {
            return null;
        }
        return CouchbaseManagerPacketSync.valueOf(name.toUpperCase());
    }

}
