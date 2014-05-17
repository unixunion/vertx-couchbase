package com.scalabl3.vertxmods.couchbase.async;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.CouchbaseClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;

/**
 * Couchbase 2.x client for vert.x<p>
 * Please see the manual for a full description<p>
 *
 * Based partially on spymemcached client for vert.x by <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 *
 * @author <a href="mailto:jasdeep@scalabl3.com">Jasdeep Jaitla</a>
 *
 */
public class CouchbaseEventBusAsync extends Verticle {
    private String eventbusAddress;
    private String cbnodes;
    private String bucket;
    private String bucketPassword;
    private int timeOutMillis;
    private int taskCheckMillis;

    private EventBus eb;
    private Logger logger;
    private ArrayList<URI> couchbaseNodes;
    private List<CouchbaseFutureContainer> pending;
    private CouchbaseClient[] couchbaseClients;
    private long timerTaskId = -1;

    @Override
    public void start() throws Exception {
        eb = vertx.eventBus();
        logger = container.getLogger();
        pending = new LinkedList<>();
        eventbusAddress = container.getConfig().getString("address", "vertx.couchbase.async");
        cbnodes = container.getConfig().getString("couchbase.nodelist");
        bucket = container.getConfig().getString("couchbase.bucket", "default");
        bucketPassword = container.getConfig().getString("couchbase.bucket.password", "");
        timeOutMillis = container.getConfig().getNumber("couchbase.timeout.ms", 10000).intValue();
        taskCheckMillis = container.getConfig().getNumber("couchbase.tasks.check.ms", 50).intValue();
        int connections = container.getConfig().getNumber("couchbase.num.clients", 1).intValue();
        // init connection pool
        connectCouchbaseClients(connections);

        // register verticle
        eb.registerHandler(eventbusAddress, memHandler, new AsyncResultHandler<Void>() {
            @Override
            public void handle(AsyncResult<Void> voidAsyncResult) {
                logger.info(this.getClass().getSimpleName() + " verticle is started");
            }
        });
    }

    Handler<Message<JsonObject>> memHandler = new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> message) {
            String command = CouchbaseCommandPacketAsync.voidNull(message.body.getString("op"));

            if (command.isEmpty()) {
                sendError(message, "\"op\" property is mandatory for request");
                return;
            }
            try {

                CouchbaseCommandPacketAsync cbcp = getByName(command);
                Future f = cbcp.operation(getCBClient(), message);

                if (f.isDone()) {
                    if (requiresAcknowledgement(message)) {
                        acknowledge(message, cbcp.buildResponse(message, f, requiresAcknowledgement(message)));
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
                sendError(message, "unknown command: '" + command + "'");
            } catch (ExecutionException e) {
                sendError(message, "operation '" + command + "' failed");
            } catch (Exception e) {
                sendError(message, e.getMessage());
            }
        }
    };

    private boolean requiresAcknowledgement(Message<JsonObject> message) {
        return message.body.getBoolean("ack") != null && message.body.getBoolean("ack") == Boolean.TRUE;
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

    private CouchbaseCommandPacketAsync getByName(String name) {
        if (name == null) {
            return null;
        }
        return CouchbaseCommandPacketAsync.valueOf(name.toUpperCase());
    }
}
