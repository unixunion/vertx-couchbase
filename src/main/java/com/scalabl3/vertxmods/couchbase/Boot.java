package com.scalabl3.vertxmods.couchbase;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

/**
 * Created by Kegan Holtzhausen on 17/05/14.
 *
 * This loads the config and then starts either the Async or Sync version of the application.
 * If you require both async and sync, like in the case where persistTo and replicateTo flags are
 * needed, then I suggest you call for two deployments, one with async true and one with false.
 *
 */
public class Boot extends Verticle {

    JsonObject config;
    private Logger logger;
    private String verticleToBoot;

    @Override
    public void start(final Future<Void> startedResult) {
        logger = container.logger();
        logger.info("mod couchbase booting...");

        config = container.config();
        logger.info("Config: " + config.toString());
        Boolean asyncMode = config.getBoolean("async_mode", false);

        if (asyncMode) {
            logger.info("Async mode");
            verticleToBoot = "com.scalabl3.vertxmods.couchbase.async.CouchbaseEventBusAsync";
        } else {
            logger.info("Sync mode");
            verticleToBoot = "com.scalabl3.vertxmods.couchbase.sync.CouchbaseEventBusSync";
        }

        container.deployVerticle(verticleToBoot, config ,new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    logger.info("successfully deployed module");
                    startedResult.setResult(null);
                } else {
                    logger.error("error deploying module, " + deployResult.cause());
                    startedResult.setFailure(deployResult.cause());
                }
            }
        });


    }



}
