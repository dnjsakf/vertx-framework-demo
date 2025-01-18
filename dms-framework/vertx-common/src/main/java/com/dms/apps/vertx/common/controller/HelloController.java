package com.dms.apps.vertx.common.controller;

import com.dms.apps.vertx.common.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.common.annotations.DmsVertxController;
import com.dms.apps.vertx.common.annotations.DmsVertxMapping;

import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsVertxController("/")
public class HelloController extends DmsAbstractVerticle {

    @DmsVertxMapping(value = "hello")
    public void handle(RoutingContext ctx){
        log.info("config: {}", config());
        // ctx.response().end("Hello~~");
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }

}
