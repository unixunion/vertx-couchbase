package com.scalabl3.vertxmods.couchbase.async;

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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

public class CouchbaseEventBusAsync extends Verticle {
    private String eventbusAddress;
    private String cbnodes;
    private String manager_username;
    private String manager_password;
    private Boolean manager_connect;
    private String bucket;
    private String bucketPassword;
    private int timeOutMillis;
    private int taskCheckMillis;

    private EventBus eb;
    private Logger logger;
    private ArrayList<URI> couchbaseNodes;
    private List<CouchbaseFutureContainer> pending;
    private CouchbaseClient[] couchbaseClients;
    private ClusterManager clusterManager;

    private long timerTaskId = -1;

    @Override
    public void start() {
        logger = container.logger();
        logger.info("starting up async connectors");
        eb = vertx.eventBus();
        pending = new LinkedList<>();
        eventbusAddress = container.config().getString("address", "vertx.couchbase.async");
        cbnodes = container.config().getString("couchbase.nodelist");
        manager_username = container.config().getString("couchbase.manager.username");
        manager_password = container.config().getString("couchbase.manager.password");
        manager_connect = container.config().getBoolean("couchbase.manager", false);
        bucket = container.config().getString("couchbase.bucket", "default");
        bucketPassword = container.config().getString("couchbase.bucket.password", "");
        timeOutMillis = container.config().getNumber("couchbase.timeout.ms", 10000).intValue();
        taskCheckMillis = container.config().getNumber("couchbase.tasks.check.ms", 50).intValue();
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

        // register verticle here, since during testing we dont have buckets yet and need the manager commands.
        eb.registerHandler(eventbusAddress, memHandler, new AsyncResultHandler<Void>() {
            @Override
            public void handle(AsyncResult<Void> voidAsyncResult) {
                logger.info(this.getClass().getSimpleName() + " verticle is started");
            }
        });
    }

    Handler<Message<JsonObject>> memHandler = new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> message) {
            String command = Util.voidNull(message.body().getString("op"));
            String management_command = Util.voidNull(message.body().getString("management"));

            if (command.isEmpty() && management_command.isEmpty()) {
                sendError(message, "\"op\" OR \"management\" property is mandatory for request");
                return;
            }

            if (!command.isEmpty() && !management_command.isEmpty()) {
                sendError(message, "cannot perform OP and MANAGEMENT at the same time, choose one!");
            }

            try {

                CouchbaseCommandPacketAsync cbcp = null;
                CouchbaseManagerPacketAsync cbmp = null;
                Future f = null;

                if (!command.isEmpty()) {
                    cbcp = getByName(command);
                    f = cbcp.operation(getCBClient(), message);
                } else if (!management_command.isEmpty()) {
                    cbmp = getMgmtByName(management_command);
                    f = cbmp.operation(getCMClient(), message);
                } else {
                    sendError(message, "unable to route command to a client / manager");
                }

                if (f.isDone()) {
                    if (requiresAcknowledgement(message)) {
                        if (!command.isEmpty()) {
                            acknowledge(message, cbcp.buildResponse(message, f, requiresAcknowledgement(message)));
                        } else if (!management_command.isEmpty()) {
                            acknowledge(message, cbmp.buildResponse(message, f, requiresAcknowledgement(message)));
                        }
                    }
                } else {
                    CouchbaseFutureContainer cbfuture = new CouchbaseFutureContainer(f, message, cbcp);
                    pending.add(cbfuture);
                    if (timerTaskId == -1) {
                        timerTaskId = vertx.setPeriodic(taskCheckMillis, new Handler<Long>() {
                            public void handle(Long timerId) {
                                for (int i = pending.size() - 1; i >= 0; i--) {
                                    CouchbaseFutureContainer cbfuture = pending.get(i);
                                    boolean sendAckowledgement = requiresAcknowledgement(cbfuture.getMessage());
                                    JsonObject r = null;
                                    if (cbfuture.getFuture().isDone()) {
                                        pending.remove(i);
                                        try {
                                            r = cbfuture.getCommand().buildResponse(cbfuture.getMessage(), cbfuture.getFuture(), sendAckowledgement);
                                        } catch (TimeoutException e) {
                                            sendError(cbfuture.getMessage(), "operation '" + cbfuture.getCommand().name().toLowerCase() + "' timed out");
                                        } catch (ExecutionException e) {
                                            sendError(cbfuture.getMessage(), "operation '" + cbfuture.getCommand().name().toLowerCase() + "' failed");
                                        } catch (Exception e) {
                                            sendError(cbfuture.getMessage(), e.getMessage());
                                        }
                                    } else if (System.currentTimeMillis() - cbfuture.getStartTime() >= timeOutMillis) {
                                        cbfuture.getFuture().cancel(true);
                                        pending.remove(i);
                                        if (sendAckowledgement) {
                                            try {
                                                r = cbfuture.getCommand().buildResponse(cbfuture.getMessage(), null, sendAckowledgement);
                                            } catch (TimeoutException e) {
                                                sendError(cbfuture.getMessage(), "operation '" + cbfuture.getCommand().name().toLowerCase() + "' timed out");
                                            } catch (ExecutionException e) {
                                                sendError(cbfuture.getMessage(), "operation '" + cbfuture.getCommand().name().toLowerCase() + "' failed");
                                            } catch (Exception e) {
                                                sendError(cbfuture.getMessage(), e.getMessage());
                                            }
                                        }
                                    }
                                    if (r != null) {
                                        acknowledge(cbfuture.getMessage(), r);
                                    }
                                }
                                if (pending.isEmpty()) {
                                    vertx.cancelTimer(timerTaskId);
                                    timerTaskId = -1;
                                }
                            }
                        });
                    }
                }

            } catch (TimeoutException e) {
                sendError(message, "operation '" + command + "' timed out");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                sendError(message, "unknown command: '" + command + "'" + management_command);
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

    private void connectClusterManager() throws IOException {
        container.logger().info("Connecting ClusterManager");
        clusterManager = new ClusterManager(ParseNodeList.getAddresses(cbnodes), manager_username, manager_password);
    }

    private ClusterManager getCMClient() {
        return clusterManager;
    }

    private void connectCouchbaseClients(int connections) throws IOException {
        container.logger().info("Connecting CouchbaseClients");
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

    private CouchbaseCommandPacketAsync getByName(String name) {
        if (name == null) {
            return null;
        }
        return CouchbaseCommandPacketAsync.valueOf(name.toUpperCase());
    }

    private CouchbaseManagerPacketAsync getMgmtByName(String name) {
        if (name == null) {
            return null;
        }
        return CouchbaseManagerPacketAsync.valueOf(name.toUpperCase());
    }

}
