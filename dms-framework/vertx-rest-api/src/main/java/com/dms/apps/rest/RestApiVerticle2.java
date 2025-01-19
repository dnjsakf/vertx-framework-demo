package com.dms.apps.rest;

import com.dms.apps.vertx.common.DmsVertxLauncher;
import com.dms.apps.vertx.common.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.common.annotations.DmsVertxController;
import com.dms.apps.vertx.common.annotations.DmsVertxMapping;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.SqlConnection;

@DmsVertxController("/common")
public class RestApiVerticle2 extends DmsAbstractVerticle {

    public static void main(String[] args) {
        new DmsVertxLauncher().start();
    }

    @DmsVertxMapping("/test2")
    public void getTest(RoutingContext ctx){
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