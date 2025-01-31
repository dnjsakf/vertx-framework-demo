package com.dms.apps.vertx.core;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxStarter {

    public static void main(String[] args) {
        new DmsVertxStarter().start();
    }

    public void start(){
        // System.setProperty("redeploy", "**/*.java");
        // System.setProperty("hazelcast.logging.type", "slf4j");
        System.setProperty("java.net.preferIPv4Stack", "true");

        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions commonFileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", "properties/vertx-common.json"));

        ConfigStoreOptions jsonFileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", "properties/vertx-config.json"));

        ConfigStoreOptions syStoreOptions = new ConfigStoreOptions()
            .setType("sys")
            .setConfig(new JsonObject().put("hierarchical", true));

        ConfigRetrieverOptions retrieverOptions = new ConfigRetrieverOptions()
            .addStore(commonFileStore)
            .addStore(jsonFileStore)
            .addStore(syStoreOptions);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, retrieverOptions);
        retriever.getConfig(ar -> {
            if( ar.succeeded() ){
                // 기본 설정
                JsonObject commonConfig = ar.result();
        
                ///////////////////////////////////////////////////////////////////////////
                // Verticle 설정
                JsonObject verticleConfig = new JsonObject()
                    .put("http", commonConfig.getJsonObject("http"))
                    .put("redis", commonConfig.getJsonObject("redis"))
                    .put("database", commonConfig.getJsonObject("database"))
                    .put("service.class", commonConfig.getString("service.class"));
                DeploymentOptions options = new DeploymentOptions().setConfig(verticleConfig);
                ///////////////////////////////////////////////////////////////////////////

                ///////////////////////////////////////////////////////////////////////////
                // Vertx 설정
                JsonObject vertxConfig = commonConfig.getJsonObject("vertx", new JsonObject());

                // VertxOptions 설정
                VertxOptions vertxOptions = new VertxOptions(vertxConfig);

                int eventLoopPoolSize = vertxConfig.getInteger("eventLoopPoolSize", VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE);
                int workerPoolSize = vertxConfig.getInteger("workerPoolSize", VertxOptions.DEFAULT_WORKER_POOL_SIZE);
                int internalBlockingPoolSize = vertxConfig.getInteger("internalBlockingPoolSize", VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE);

                vertxOptions.setEventLoopPoolSize(eventLoopPoolSize);
                vertxOptions.setWorkerPoolSize(workerPoolSize);
                vertxOptions.setInternalBlockingPoolSize(internalBlockingPoolSize);
                ///////////////////////////////////////////////////////////////////////////
                
                // HttpServer Verticle 배포
                vertx.deployVerticle(new DmsVertxVerticle(), options, res -> {
                    if( res.succeeded() ){
                        log.info("Deployed HttpServer Verticle!!!");
                    } else {
                        res.cause().printStackTrace();
                    }
                });

                // EventBus Verticle 배포
                vertx.deployVerticle(new DmsVertxEventBus(), options, res -> {
                    if( res.succeeded() ){
                        log.info("Deployed EventBus Verticle!!!");
                    } else {
                        res.cause().printStackTrace();
                    }
                });
            } else {
                log.error(ar.cause().getMessage(), ar.cause());
            }
        });
        
    }
    
}
