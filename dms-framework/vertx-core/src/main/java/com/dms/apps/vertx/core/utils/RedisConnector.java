package com.dms.apps.vertx.core.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;

public class RedisConnector {
    
    private static final int MAX_RECONNECT_RETRIES = 16;

    private Vertx vertx;

    private final RedisOptions options = new RedisOptions();
    private Redis redis;
    private RedisConnection client;
    private final AtomicBoolean CONNECTING = new AtomicBoolean();

    public RedisConnector(Vertx vertx){
        this.vertx = vertx;
    }

    /**
     * Will create a redis client and setup a reconnect handler when there is
     * an exception in the connection.
     */
    private Future<RedisConnection> createRedisClient() {
        Promise<RedisConnection> promise = Promise.promise();

        // make sure to invalidate old connection if present
        if (redis != null) {
            redis.close();;
        }

        if (CONNECTING.compareAndSet(false, true)) {
            redis = Redis.createClient(vertx, options);
            redis.connect()
                .onSuccess(conn -> {
                    client = conn;
                    // make sure the client is reconnected on error
                    // eg, the underlying TCP connection is closed but the client side doesn't know it yet
                    //     the client tries to use the staled connection to talk to server. An exceptions will be raised
                    conn.exceptionHandler(e -> {
                        attemptReconnect(0);
                    });

                    // make sure the client is reconnected on connection close
                    // eg, the underlying TCP connection is closed with normal 4-Way-Handshake
                    //     this handler will be notified instantly
                    conn.endHandler(placeHolder -> {
                        attemptReconnect(0);
                    });

                    // allow further processing
                    promise.complete(conn);
                    CONNECTING.set(false);
                }).onFailure(t -> {
                    promise.fail(t);
                    CONNECTING.set(false);
                });
        } else {
            promise.complete();
        }

        return promise.future();
    }

    /**
     * Attempt to reconnect up to MAX_RECONNECT_RETRIES
     */
    private void attemptReconnect(int retry) {
        if (retry > MAX_RECONNECT_RETRIES) {
            // we should stop now, as there's nothing we can do.
            CONNECTING.set(false);
        } else {
            // retry with backoff up to 10240 ms
            long backoff = (long) (Math.pow(2, Math.min(retry, 10)) * 10);

            vertx.setTimer(backoff, timer -> {
                createRedisClient()
                    .onFailure(t -> attemptReconnect(retry + 1));
            });
        }
    }
}
