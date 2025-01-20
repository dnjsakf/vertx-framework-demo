package com.dms.apps.vertx.core.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsRedisClient {
    
    private static final int MAX_RECONNECT_RETRIES = 16;

    private Vertx vertx;
    private JsonObject config;

    private Redis redis;
    private RedisAPI redisApi;
    private RedisOptions redisOptions;
    
    private final AtomicBoolean CONNECTING = new AtomicBoolean();

    public DmsRedisClient(Vertx vertx, JsonObject config) throws NullPointerException {
        if( vertx == null ){
            throw new NullPointerException("Vertx Instance is null");
        }
        if( config == null ){
            throw new NullPointerException("Redis Configuration is null");
        }
        this.vertx = vertx;
        this.config = config;
        this.redisOptions = new RedisOptions(this.config)
            .setType(RedisClientType.STANDALONE);
            
        log.debug("redisOptions:");
        log.debug(this.redisOptions.toJson().encodePrettily());
    }

    public Future<RedisAPI> getApi(){
        Promise<RedisAPI> promise = Promise.promise();

        if( this.redisApi != null ){
            promise.complete(this.redisApi);    
        } else {
            promise.fail(new NullPointerException("RedisAPI Instance is null"));
        }

        return promise.future();
    }

    public Future<Response> ping(){
        return this.redisApi.send(Command.PING);
    }

    public Future<RedisAPI> connect() {
        return connect(0);
    }

    private Future<RedisAPI> connect(int retry) {
        Promise<RedisAPI> promise = Promise.promise();

        if ( this.redis != null ) {
            this.redis.close();;
        }
        
        if (CONNECTING.compareAndSet(false, true)) {
            this.redis = Redis.createClient(this.vertx, this.redisOptions);
            this.redis.connect()
                .onSuccess(conn -> {
                    this.redisApi = RedisAPI.api(this.redis);
                    promise.complete(this.redisApi);
                    CONNECTING.set(false);
                })
                .onFailure(t -> {
                    promise.fail(t);
                    CONNECTING.set(false);
                });
        } else {
            if (retry > MAX_RECONNECT_RETRIES) {
                // we should stop now, as there's nothing we can do.
                CONNECTING.set(false);
            } else {
                long backoff = (long) (Math.pow(2, Math.min(1, 10)) * 10);
                vertx.setTimer(backoff, id -> {
                    log.info("retry: {}", retry);
                    connect(retry + 1).onComplete(promise);
                });
            }
        }

        return promise.future();
    }

}
