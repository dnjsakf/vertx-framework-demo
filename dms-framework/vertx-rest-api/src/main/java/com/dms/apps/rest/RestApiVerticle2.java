package com.dms.apps.rest;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;

@DmsController("/common")
public class RestApiVerticle2 extends DmsAbstractVerticle {

    @DmsInject
    private Pool pool;

    @DmsRequestMapping("/test2")
    public void getTest(RoutingContext ctx){
        pool.getConnection(ar -> {
            if( ar.succeeded() ){
                SqlConnection conn = ar.result();
                conn.query("select 1")
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