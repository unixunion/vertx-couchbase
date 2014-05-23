package com.scalabl3.vertxmods.couchbase;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

/**
 * Created by marzubus on 17/05/14.
 *
 * This loads the config and then starts either the Async or Sync version of the application
 *
 */
public class Boot extends Verticle {

    JsonObject config;
    private Logger logger;
    private String verticleToBoot;

    @Override
    public void start(final Future<Void> startedResult) {
        logger = container.logger();
        logger.info("Couchbase MOD Booting...");

        config = container.config();

        logger.info("Config: " + config.toString());
        Boolean asyncMode = config.getBoolean("async_mode", false);

        logger.info("asyncMode: " + asyncMode.toString());

        if (asyncMode) {
            verticleToBoot = "com.scalabl3.vertxmods.couchbase.async.CouchbaseEventBusAsync";
        } else {
            verticleToBoot = "com.scalabl3.vertxmods.couchbase.sync.CouchbaseEventBusSync";
        }

        container.deployVerticle(verticleToBoot, config ,new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    startedResult.setResult(null);
                } else {
                    startedResult.setFailure(deployResult.cause());
                }
            }
        });

        container.deployVerticle("com.scalabl3.vertxmods.couchbase.ClusterManager", config ,new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    startedResult.setResult(null);
                } else {
                    startedResult.setFailure(deployResult.cause());
                }
            }
        });


    }



}
