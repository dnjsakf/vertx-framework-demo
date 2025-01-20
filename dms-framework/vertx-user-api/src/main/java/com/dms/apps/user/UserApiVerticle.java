package com.dms.apps.user;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;

import io.vertx.ext.web.RoutingContext;

@DmsController("/user")
public class UserApiVerticle extends DmsAbstractVerticle {

    @DmsRequestMapping("/")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
}