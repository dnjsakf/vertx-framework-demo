package com.dms.apps.rest;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;

import io.vertx.ext.web.RoutingContext;

@DmsController("/common")
public class RestApiVerticle extends DmsAbstractVerticle {

    @DmsRequestMapping("/test")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
}