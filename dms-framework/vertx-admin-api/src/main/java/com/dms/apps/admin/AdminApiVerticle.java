package com.dms.apps.admin;

import com.dms.apps.vertx.core.DmsVertxLauncher;
import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsVertxController;
import com.dms.apps.vertx.core.annotations.DmsVertxMapping;

import io.vertx.ext.web.RoutingContext;

@DmsVertxController("/admin")
public class AdminApiVerticle extends DmsAbstractVerticle {

    @DmsVertxMapping("/")
    public void getTest(RoutingContext ctx){
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(config().toString());
    }
    
}