package com.dms.apps.vertx.core.utils;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsDBClient {

    private Vertx vertx;
    private JsonObject config;

    private Pool pool;
    private PoolOptions poolOptions;
    private PgConnectOptions connectOptions;

    public DmsDBClient(Vertx vertx, JsonObject config){
        if( vertx == null ){
            throw new NullPointerException("Vertx Instance is null");
        }
        if( config == null ){
            throw new NullPointerException("Redis Configuration is null");
        }
        this.vertx = vertx;
        this.config = config;

        this.poolOptions = new PoolOptions(this.config);
        this.connectOptions = new PgConnectOptions(this.config);

        log.debug("poolOptions:");
        log.debug(this.poolOptions.toJson().encodePrettily());
        
        log.debug("connectOptions:");
        log.debug(this.connectOptions.toJson().encodePrettily());
    }

    public Future<SqlConnection> getConnection(){
        Promise<SqlConnection> poolPromise = Promise.promise();

        if( this.pool != null ){
            this.pool.getConnection().onComplete(poolPromise);
        } else {
            poolPromise.fail(new NullPointerException("Pool Instance is null."));
        }

        return poolPromise.future();
    }

    public Future<JsonArray> execute(String query, Map<String, Object> params){
        Promise<JsonArray> executePromise = Promise.promise();
        
        if( this.pool != null ){
            this.pool.getConnection().onSuccess(conn -> {
                SqlTemplate.forQuery(conn, query)
                    .mapTo(row -> row.toJson())
                    .execute(params)
                    .onSuccess(records -> {
                        JsonArray rows = new JsonArray();
                        for(JsonObject row : records){
                            rows.add(row);
                        }
                        executePromise.complete(rows);
                    })
                    .onFailure(t -> {
                        conn.close();
                        executePromise.fail(t);
                    });
            })
            .onFailure(t -> {
                executePromise.fail(t);
            });
        } else {
            executePromise.fail(new NullPointerException("Pool Instance is null."));
        }

        return executePromise.future();
    }

    /**
     * DB Connection Pool 생성
     */
    public Future<Pool> connect() {
        Promise<Pool> poolPromise = Promise.promise();

        Pool pool = PgBuilder.pool()
            .with(this.poolOptions)
            .connectingTo(this.connectOptions)
            .using(vertx)
            .build();

        log.debug("connectOptions: {}", connectOptions.toJson().encodePrettily());
        log.debug("poolOptions: {}", poolOptions.toJson().encodePrettily());

        pool.getConnection().compose(conn -> {
            this.pool = pool;
            return conn.query("SELECT 1")
                .execute()
                .compose(res -> {
                    log.info("Connection pool is successfully connected.");
                    conn.close();
                    return Future.succeededFuture();
                });
        }).onComplete(ar -> {
            if( ar.succeeded() ){
                poolPromise.complete(pool);
            } else {
                poolPromise.fail(ar.cause());
            }
        });

        return poolPromise.future();
    }
}
