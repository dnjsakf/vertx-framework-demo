package com.dms.apps.vertx.core;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;

import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.annotations.DmsVertxController;
import com.dms.apps.vertx.core.annotations.DmsVertxMapping;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmsVertxVerticle extends AbstractVerticle {

    private static final String[] DEFAULT_METHODS = new String[]{
        HttpMethod.OPTIONS.name(),
        HttpMethod.GET.name(),
        HttpMethod.POST.name(),
        HttpMethod.PUT.name(),
        HttpMethod.DELETE.name()
    };

    private Set<String> registeredRoutes = new HashSet<>();

    private Pool pool;
    private String httpHost;
    private Integer httpPort;

    public Pool getPool(){
        return this.pool;
    }
    public String getHttpHost(){
        return this.httpHost;
    }
    public Integer getHttpPost(){
        return this.httpPort;
    }
    
    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        DmsVertxLauncher.getConfig(ar -> {
            if( ar.succeeded() ){
                JsonObject config = ar.result();
                String httpHost = config.getString("http.host", "localhost");
                Integer httpPort = config.getInteger("http.port", 8080);
                
                this.httpHost = httpHost;
                this.httpPort = httpPort;

                try {
                    Future.succeededFuture().compose(res -> {
                        // DB 연결
                        return createConnectionPool(config).compose(pool -> {
                            log.info("Created Connection Pool");
                            this.pool = pool;
                            return Future.succeededFuture();
                        });
                    })
                    .compose(res ->{
                        // Router 생성
                        return createRouter(config).compose(router -> {
                            log.info("Created Router");
                            return Future.succeededFuture(router);
                        });
                    })
                    .compose(router -> {
                        // 서버 실행
                        Promise<Void> promise = Promise.promise();
                        vertx.createHttpServer()
                            .requestHandler(router)
                            .listen(httpPort, httpHost, http -> {
                                if (http.succeeded()) {
                                    log.info("Started HTTP Server: http://"+httpHost+":"+httpPort);
                                    promise.complete();
                                } else {
                                    promise.fail(http.cause());
                                }
                            });
                        return promise.future();
                    })
                    .onComplete(ar2 -> {
                        if( ar2.succeeded() ){
                            startPromise.complete();
                        } else {
                            startPromise.fail(ar2.cause());
                        }
                    });
                    // .onSuccess(res -> {
                    //     startPromise.complete();
                    // })
                    // .onFailure(t -> {
                    //     startPromise.fail(t);
                    // });

                } catch ( Exception e ){
                    startPromise.fail(e);
                }
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }

    /**
     * DB Connection Pool 생성
     */
    private Future<Pool> createConnectionPool(JsonObject config) {
        Promise<Pool> poolPromise = Promise.promise();

        JsonObject databaseConfig = config.getJsonObject("database", new JsonObject());

        PgConnectOptions connectOptions = new PgConnectOptions()
            .setPort(databaseConfig.getInteger("port", 5432))
            .setHost(databaseConfig.getString("host", "localhost"))
            .setDatabase(databaseConfig.getString("database", "postgres"))
            .setUser(databaseConfig.getString("username", "postgres"))
            .setPassword(databaseConfig.getString("password", "postgres"));

        PoolOptions poolOptions = new PoolOptions()
            .setMaxSize(databaseConfig.getInteger("pool-size", 5));

        Pool pool = PgBuilder.pool()
            .with(poolOptions)
            .connectingTo(connectOptions)
            .using(vertx)
            .build();

        log.debug("connectOptions: {}", connectOptions.toJson().encodePrettily());
        log.debug("poolOptions: {}", poolOptions.toJson().encodePrettily());

        pool.getConnection().compose(conn -> {
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

    private Future<Void> createRedisClient(JsonObject config){
        Promise<Void> promise = Promise.promise();
        RedisOptions redisOptions = new RedisOptions()
            .setConnectionString("redis://localhost:6379")
            // allow at max 8 connections to redis
            .setMaxPoolSize(8)
            // allow 32 connection requests to queue waiting
            // for a connection to be available.
            .setMaxWaitingHandlers(32);

        Redis client = Redis.createClient(vertx, redisOptions);

        client.connect(onConnect -> {
        if (onConnect.succeeded()) {
            RedisConnection connection = onConnect.result();
            connection.send(Request.cmd(Command.SET).arg("key").arg("value"), onSet -> {
            if (onSet.succeeded()) {
                System.out.println("Key set successfully");
                connection.send(Request.cmd(Command.GET).arg("key"), onGet -> {
                if (onGet.succeeded()) {
                    System.out.println("Value: " + onGet.result().toString());
                } else {
                    System.out.println("Failed to get key: " + onGet.cause().getMessage());
                }
                });
            } else {
                System.out.println("Failed to set key: " + onSet.cause().getMessage());
            }
            });
        } else {
            System.out.println("Failed to connect to Redis: " + onConnect.cause().getMessage());
        }
        });

        return promise.future();
    }
    
    /**
     * 라우터 생성
     */
    private Future<Router> createRouter(JsonObject config) {
        Promise<Router> routerPromise = Promise.promise();
        try {
            Router router = Router.router(vertx);
    
            router.route().handler(BodyHandler.create());
    
            setDynamicRoutes(router, config);

            routerPromise.complete(router);
        } catch ( Exception e ){
            routerPromise.fail(e);
        }
        return routerPromise.future();
    }
  
    /**
     * DmsAbstractVerticle이 적용된 클래스에서
     * DmsVertxMapping 어노테이션이 적용된 메소드를 찾아서
     * 라우팅 처리
     * @param router
     */
    private void setDynamicRoutes(Router router, JsonObject config) throws Exception {
        Reflections reflections = new Reflections(config.getString("service.class", "com.dms.apps.vertx"));
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(DmsVertxController.class);

        // 어노테이션을 적용받은 클래스들을 찾아 라우팅 설정
        for (Class<?> cls : classes) {
            if (cls.isAnnotationPresent(DmsVertxController.class)) {
                DmsVertxController annotation = cls.getAnnotation(DmsVertxController.class);
                final String path = annotation.value();

                // Verticle 배포
                DeploymentOptions options = new DeploymentOptions().setConfig(config);
                DmsAbstractVerticle verticleInstance = (DmsAbstractVerticle) cls.getDeclaredConstructor().newInstance();

                verticleInstance.setPool(this.pool);
                vertx.deployVerticle(verticleInstance, options);

                for (Method method : cls.getDeclaredMethods()) {
                    if (method.isAnnotationPresent(DmsVertxMapping.class)) {
                        // Method Annotation 처리
                        DmsVertxMapping mappingAnnotation = method.getAnnotation(DmsVertxMapping.class);
                        
                        // 중복된 Route가 존재하는지 체크
                        String fullPath = path + mappingAnnotation.value();
                        if (registeredRoutes.contains(fullPath)) {
                            throw new IllegalArgumentException("중복 경로가 발견되었습니다: " + fullPath);
                        } else {
                            registeredRoutes.add(fullPath);
                        }
                        
                        // Annotation에 작성된 Methods 목록 가져오기
                        String[] mappingMethods = mappingAnnotation.methods();
                        if( mappingMethods == null || mappingMethods.length == 0 ){
                            mappingMethods = DEFAULT_METHODS;
                        }
                        Set<HttpMethod> httpMethods = (Set<HttpMethod>) Arrays.stream(mappingMethods)
                            .map(httpMethod -> { 
                                return new HttpMethod(httpMethod);
                            }).collect(Collectors.toSet());

                        // 등록된 경로 로그 출력
                        log.info("Route: http://{}:{}{}, Allow: {}", this.httpHost, this.httpPort, fullPath, httpMethods);

                        // Vertx Route 생성
                        for(HttpMethod httpMethod : httpMethods){
                            Route route = router.route(fullPath);
                            route.method(httpMethod);
                            route.handler(CorsHandler.create().addOrigin("*").allowedMethod(httpMethod));
                            route.handler(ctx -> {
                                try {
                                    log.info("[{}] {}", ctx.request().method().name(), ctx.request().path());
                                    method.invoke(verticleInstance, ctx);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            route.failureHandler(ctx -> {
                                ctx.response().end(ctx.failure().getMessage());
                            });
                        }
                    }
                }
            }
        }
    }

}
