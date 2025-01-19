package com.dms.apps.vertx.common.controller;

import com.dms.apps.vertx.common.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.common.annotations.DmsVertxController;
import com.dms.apps.vertx.common.annotations.DmsVertxMapping;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DmsVertxController("/")
public class HelloController extends DmsAbstractVerticle {

    @DmsVertxMapping(value = "hello")
    public void handle(RoutingContext ctx){
        log.info("config: {}", config());

        pool().getConnection(ar -> {
            if( ar.succeeded() ){
                SqlConnection conn = ar.result();
                conn.query("select * from DMS_COMM_USER")
                    .execute()
                    .onComplete(ar2 -> {
                        if( ar2.succeeded() ){
                            JsonArray rows = new JsonArray();
                            ar2.result().forEach(row -> rows.add(row.toJson()));
                            ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(rows.encodePrettily());
                        } else {
                            ar2.cause().printStackTrace();
                            ctx.fail(ar2.cause());
                        }
                    });
            } else {
                ar.cause().printStackTrace();
                ctx.fail(ar.cause());
            }
        });
    }

}
