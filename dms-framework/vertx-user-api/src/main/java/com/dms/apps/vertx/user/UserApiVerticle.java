package com.dms.apps.vertx.user;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;

import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsController("/user")
public class UserApiVerticle extends DmsAbstractVerticle {

    @Override
    public void start() throws Exception {
        // EventBus 사용
        vertx.eventBus().consumer("eventbus.user", message -> {
            log.info("Received message: " + message.body());
            message.reply("pong");
        });
    }

    @DmsRequestMapping("/")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }

    @DmsRequestMapping("/eventbus")
    public void getEventBus(RoutingContext ctx){
        vertx.eventBus().request("eventbus.user", "I'm User")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }

    @DmsRequestMapping("/eventbus/admin")
    public void getEventBusAdmin(RoutingContext ctx){
        vertx.eventBus().request("eventbus.admin", "I'm User")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }

    @DmsRequestMapping("/eventbus/ping")
    public void getEventBusPing(RoutingContext ctx){
        vertx.eventBus().request("eventbus.ping", "I'm User")
            .onSuccess(message -> {
                ctx.response().end(message.body().toString());
            })
            .onFailure(t -> {
                ctx.fail(t);
            });
    }
}