package com.dms.apps.vertx.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxStarter {

    public static void main(String[] args) {
        new DmsVertxStarter().start();
    }

    public void start(){
        // System.setProperty("redeploy", "**/*.java");
        System.setProperty("hazelcast.logging.type", "slf4j");

        Vertx tempVertx = Vertx.vertx();

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

        ConfigRetriever retriever = ConfigRetriever.create(tempVertx, retrieverOptions);
        retriever.getConfig(ar -> {
            if( ar.succeeded() ){
                // 임시 Vertx Instance는 종료
                tempVertx.close()
                .onFailure(t -> log.error(t.getMessage(), t))
                .onSuccess(ar2 -> {
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

                    ///////////////////////////////////////////////////////////////////////////
                    // HazelCast 설정
                    JsonObject hazelcastOptions = commonConfig.getJsonObject("hazelcast", new JsonObject());

                    NetworkConfig networkConfig = new NetworkConfig()
                        .setPort(hazelcastOptions.getInteger("network.port", 5700))
                        // .setPortAutoIncrement(false)
                        .setPublicAddress(hazelcastOptions.getString("public.address","10.100.100.23"));

                    // TCP/IP 설정
                    JoinConfig joinConfig = networkConfig.getJoin();
                    joinConfig.getTcpIpConfig()
                            .setEnabled(true);
                        
                    Config hazelcastConfig = new Config()
                        .setClusterName(hazelcastOptions.getString("cluster.name", "dms.hazel.cluster"))
                        .setNetworkConfig(networkConfig);

                    // HazelCast 클러스터 매니저 설정
                    HazelcastClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
                    ///////////////////////////////////////////////////////////////////////////
                    
                    // Vertx 클러스터 빌더
                    Vertx.builder()
                        .with(vertxOptions)
                        .withClusterManager(mgr)
                        .buildClustered()
                        .onFailure(t -> log.error(t.getMessage(), t))
                        .onSuccess(clusteredVertx -> {

                            // JVM 종료 훅 추가
                            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                                log.info("Shutdown Hook is running!");
                                // 데이터베이스 및 Redis 클라이언트 종료 로직 추가
                                clusteredVertx.close();
                            }));

                            // HttpServer Verticle 배포
                            clusteredVertx.deployVerticle(new DmsVertxVerticle(), options, res -> {
                                if( res.succeeded() ){
                                    log.info("Deployed HttpServer Verticle!!!");
                                } else {
                                    res.cause().printStackTrace();
                                }
                            });

                            // EventBus Verticle 배포
                            clusteredVertx.deployVerticle(new DmsVertxEventBus(), options, res -> {
                                if( res.succeeded() ){
                                    log.info("Deployed EventBus Verticle!!!");
                                } else {
                                    res.cause().printStackTrace();
                                }
                            });
                        });
                });
            } else {
                log.error(ar.cause().getMessage(), ar.cause());
            }
        });
        
    }
    
}
