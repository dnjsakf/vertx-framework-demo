package com.dms.apps.rest;

import com.dms.apps.vertx.common.DmsVertxLauncher;
import com.dms.apps.vertx.common.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.common.annotations.DmsVertxController;
import com.dms.apps.vertx.common.annotations.DmsVertxMapping;

import io.vertx.ext.web.RoutingContext;

@DmsVertxController("/common")
public class RestApiVerticle2 extends DmsAbstractVerticle {

    public static void main(String[] args) {
        new DmsVertxLauncher().start();
    }

    @DmsVertxMapping("/test")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
}