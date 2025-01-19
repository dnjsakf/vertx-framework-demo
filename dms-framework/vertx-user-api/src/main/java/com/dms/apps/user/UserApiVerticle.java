package com.dms.apps.user;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsVertxController;
import com.dms.apps.vertx.core.annotations.DmsVertxMapping;

import io.vertx.ext.web.RoutingContext;

@DmsVertxController("/user")
public class UserApiVerticle extends DmsAbstractVerticle {

    @DmsVertxMapping("/")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
}