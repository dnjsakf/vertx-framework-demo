package com.dms.apps.vertx.admin;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;
import com.dms.apps.vertx.core.utils.DmsDBClient;
import com.dms.apps.vertx.core.utils.DmsRedisClient;

import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsController("/admin")
public class AdminApiVerticle extends DmsAbstractVerticle {

    @DmsInject
    private DmsDBClient dbClient;

    @DmsInject
    private DmsRedisClient redisClient;

    @Override
    public void start() throws Exception {
        // EventBus 사용
        vertx.eventBus().consumer("eventbus.admin", message -> {
            log.info("Received message: " + message.body());
            message.reply("pong");
        });
    }

    @DmsRequestMapping(value = "/config", methods = { "GET" })
    public void getConfig(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }

    @DmsRequestMapping(value = "/redis", methods = { "GET" })
    public void getTest(RoutingContext ctx){
        dbClient.getConnection()
            .onSuccess(conn -> {
                conn.query("SELECT 1")
                    .execute()
                    .onSuccess(rows -> {
                        conn.close();
                        ctx.response()
                            .putHeader("Content-Type", "application/json")
                            .end(config().toString());
                    })
                    .onFailure(t -> {
                        conn.close();
                        ctx.fail(t);
                    });
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }

    @DmsRequestMapping(value = "/db", methods = { "GET" })
    public void getTest2(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
    
    @DmsRequestMapping("/eventbus/ping")
    public void getEventBusPing(RoutingContext ctx){
        vertx.eventBus().request("eventbus.ping", "I'm Admin")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }

    @DmsRequestMapping("/eventbus/user")
    public void getEventBusUser(RoutingContext ctx){
        vertx.eventBus().request("eventbus.user", "I'm Admin")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }

    @DmsRequestMapping("/eventbus")
    public void getEventBus(RoutingContext ctx){
        vertx.eventBus().request("eventbus.user", "I'm Admin")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }
    
}