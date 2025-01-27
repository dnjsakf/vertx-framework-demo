package com.dms.apps.vertx.core.examples;

import java.util.HashMap;
import java.util.Map;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;
import com.dms.apps.vertx.core.utils.DmsDBClient;
import com.dms.apps.vertx.core.utils.DmsRedisClient;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsController("")
public class SampleController extends DmsAbstractVerticle {

    @DmsInject
    private DmsDBClient dbClient;

    @DmsInject
    private DmsRedisClient redisClient;

    @DmsRequestMapping(value = "/hello", methods = { "GET" })
    public void getIndex(RoutingContext ctx){
        ctx.response().end("Hello, Vertx!!!");
    }

    @DmsRequestMapping(value = "/eb/:key", methods = { "GET" })
    public void getEventBus(RoutingContext ctx){
        String key = ctx.pathParam("key");
        String data = ctx.queryParam("data").get(0);
        // vertx.eventBus().request(key, data, ar -> {
        //     if( ar.succeeded() ){
        //         ctx.response().end(ar.result().body().toString());
        //     } else {
        //         ctx.fail(ar.cause());
        //     }
        // });
        redisClient.rpush(key, data).onSuccess(resp -> {
            ctx.response().end(resp.toString());
        }).onFailure(t -> {
            ctx.fail(t);
        });
    }

    @DmsRequestMapping("/ping")
    public void getPing(RoutingContext ctx){
        redisClient.ping().onComplete(ar2 -> {
            if( ar2.succeeded() ){
                Response resp = ar2.result();
                ctx.response().end(resp.toString());
            } else {
                ctx.fail(ar2.cause());
            }
        });
    }
    
    @DmsRequestMapping(value = "/db/ping")
    public void getDBPingPong(RoutingContext ctx){

        Map<String, Object> params = new HashMap<String, Object>(); // = null;

        dbClient.execute("SELECT 'PONG' AS PING_PONG", params).onComplete(ar2 -> {
            if( ar2.succeeded() ){
                JsonArray rows = ar2.result();
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end(rows.encodePrettily());
            } else {
                ctx.fail(ar2.cause());
            }
        });
    }

    @DmsRequestMapping("/redis/ping")
    public void getRedisPingPong(RoutingContext ctx){
        redisClient.getApi().onComplete(ar -> {
            if( ar.succeeded() ){
                RedisAPI client = ar.result();
                client.send(Command.PING).onComplete(ar2 -> {
                    if( ar2.succeeded() ){
                        Response resp = ar2.result();
                        ctx.response().end(resp.toString());
                    } else {
                        ctx.fail(ar2.cause());
                    }
                });
            } else {
                ctx.fail(ar.cause());
            }
        });
    }

}
