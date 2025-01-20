package com.dms.apps.vertx.core;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxLauncher extends Launcher {

    private static ConfigRetriever retriever;

    private Vertx newVertx;

    public static void main(String[] args) {
        new DmsVertxLauncher().start();
    }

    public void start(){
        dispatch(new String[]{
            "run", "java:"+DmsVertxVerticle.class.getCanonicalName()
        });
    }

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        log.info("1. beforeStartingVertx");

        newVertx = Vertx.vertx();

        ConfigStoreOptions commonFileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", "properties/vertx-common.json"));

        ConfigStoreOptions jsonFileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", "properties/vertx-config.json"));

        ConfigRetrieverOptions retrieverOptions = new ConfigRetrieverOptions()
            .addStore(commonFileStore)
            .addStore(jsonFileStore);

        retriever = ConfigRetriever.create(newVertx, retrieverOptions);
        retriever.getConfig(ar -> {
        if( ar.succeeded() ){
            log.info("Options Loaded");
            JsonObject config = ar.result();

            int eventLoopPoolSize = config.getInteger("eventLoopPoolSize", VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE);
            int workerPoolSize = config.getInteger("workerPoolSize", VertxOptions.DEFAULT_WORKER_POOL_SIZE);
            int internalBlockingPoolSize = config.getInteger("internalBlockingPoolSize", VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE);

            options.setEventLoopPoolSize(eventLoopPoolSize);
            options.setWorkerPoolSize(workerPoolSize);
            options.setInternalBlockingPoolSize(internalBlockingPoolSize);

        } else {
            log.error(ar.cause().getMessage(), ar.cause());
        }
        });
    }

    @Override
    public void afterStartingVertx(Vertx vertx) {
        log.info("2. afterStartingVertx");

        getConfig(ar -> {
            if( ar.succeeded() ){
                JsonObject config = ar.result();
                DeploymentOptions eventBusOptions = new DeploymentOptions();
                eventBusOptions.setConfig(config.getJsonObject("redis", new JsonObject()));
                vertx.deployVerticle(DmsVertxEventBus.class, eventBusOptions);
            }
        });
    }

    @Override
    public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
        log.info("3. beforeDeployingVerticle");
    }

    @Override
    public void beforeStoppingVertx(Vertx vertx) {
        log.info("4. beforeStoppingVertx");
        newVertx.close(ar -> {
        if( ar.succeeded() ){
            log.info("Bye...");
        } else {
            log.error(ar.cause().getMessage(), ar.cause());
        }
        });
    }
    
    public static void getConfig(Handler<AsyncResult<JsonObject>> completionHandler){
        retriever.getConfig(completionHandler);
    }

    public Vertx getVertx(){
        return newVertx;
    }

}
