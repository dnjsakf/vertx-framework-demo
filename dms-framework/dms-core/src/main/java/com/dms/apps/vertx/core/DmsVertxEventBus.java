package com.dms.apps.vertx.core;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxEventBus extends AbstractVerticle {

    private Redis redis;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        log.info("Deploying EventBus");
        log.debug(config().encodePrettily());
        
        RedisOptions redisOptions = new RedisOptions(config())
            .setType(RedisClientType.STANDALONE);

        this.redis = Redis.createClient(vertx, redisOptions);
        this.redis.connect().onComplete(ar -> {
            if( ar.succeeded() ){
                log.info("Redis Connected");
            } else {
                log.error(ar.cause().getMessage(), ar.cause());
            }
        });

        vertx.eventBus().consumer("redis.connect", message -> {
            this.redis.connect(ar -> {
                if (ar.succeeded()) {
                    RedisConnection connection = ar.result();
                    JsonObject connectionJson = new JsonObject()
                        .put("connection", connection);
                    message.reply(connectionJson);
                } else {
                    message.fail(500, "Failed to connect to Redis");
                }
            });
        });
    }

    @Override
    public void stop() throws Exception {
        if( redis != null ){
            redis.close();
        }
    }
    
}
