package com.dms.apps.vertx.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;

import com.dms.apps.vertx.core.abs.DmsAbstractBase;
import com.dms.apps.vertx.core.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.core.abs.DmsAbstractWorker;
import com.dms.apps.vertx.core.annotations.DmsController;
import com.dms.apps.vertx.core.annotations.DmsInject;
import com.dms.apps.vertx.core.annotations.DmsRequestMapping;
import com.dms.apps.vertx.core.annotations.DmsSubscribe;
import com.dms.apps.vertx.core.utils.DmsDBClient;
import com.dms.apps.vertx.core.utils.DmsRedisClient;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.spi.cluster.hazelcast.ClusterHealthCheck;
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

    private String httpHost;
    private Integer httpPort;

    private DmsDBClient dmsDBClient;
    private DmsRedisClient dmsRedisClient;
    
    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        JsonObject config = config();

        JsonObject httpConfig = config.getJsonObject("http", new JsonObject());
        String httpHost = httpConfig.getString("host", "localhost");
        Integer httpPort = httpConfig.getInteger("port", 8080);
        
        this.httpHost = httpHost;
        this.httpPort = httpPort;

        try {
            Future.succeededFuture().compose(res -> {
                // DB 연결
                return createDBClient(config).compose(dmsDBClient -> {
                    log.info("Created Connection Pool");
                    // this.pool = pool;
                    this.dmsDBClient = dmsDBClient;
                    return Future.succeededFuture();
                });
            })
            .compose(res -> {
                // REDIS 연결
                return createRedisClient(config).compose(dmsRedisClient -> {
                    log.info("Created Redis Client");
                    this.dmsRedisClient = dmsRedisClient;
                    return Future.succeededFuture();
                });
            })
            .compose(res -> {
                // 서버 실행
                Promise<Void> promise = Promise.promise();
                try {                        
                    // Router 생성
                    createRouter(config).onComplete(ar2 -> {
                        if( ar2.succeeded() ){
                            Router router = ar2.result();
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
                        } else {
                            promise.fail(ar2.cause());
                        }
                        log.info("Created Router");
                    });
                } catch ( Exception e ){
                    promise.fail(e);
                }
                return promise.future();
            })
            .onComplete(ar2 -> {
                if( ar2.succeeded() ){
                    startPromise.complete();
                } else {
                    startPromise.fail(ar2.cause());
                }
            });

        } catch ( Exception e ){
            startPromise.fail(e);
        }
    }

    /**
     * DB Connection Pool 생성
     */
    private Future<DmsDBClient> createDBClient(JsonObject config) {
        Promise<DmsDBClient> dbPromise = Promise.promise();

        try {
            JsonObject databaseConfig = config.getJsonObject("database", new JsonObject());
            DmsDBClient dbClient = new DmsDBClient(vertx, databaseConfig);
            dbClient.connect().onComplete(ar -> {
                if( ar.succeeded() ){
                    dbPromise.complete(dbClient);
                } else {
                    dbPromise.fail(ar.cause());
                }
            });

        } catch ( NullPointerException e ){
            dbPromise.fail(e);
        }

        return dbPromise.future();
    }

    private Future<DmsRedisClient> createRedisClient(JsonObject config){
        Promise<DmsRedisClient> redisPromise = Promise.promise();

        try {
            JsonObject redisConfig = config.getJsonObject("redis", new JsonObject());
            DmsRedisClient redisClient = new DmsRedisClient(vertx, redisConfig);
            redisClient.connect().onComplete(ar -> {
                if( ar.succeeded() ){
                    redisPromise.complete(redisClient);
                } else {
                    redisPromise.fail(ar.cause());
                }
            });

        } catch ( NullPointerException e ){
            redisPromise.fail(e);
        }

        return redisPromise.future();
    }
    
    /**
     * 라우터 생성
     */
    private Future<Router> createRouter(JsonObject config) {
        Promise<Router> routerPromise = Promise.promise();
        try {
            Router mainRouter = Router.router(vertx);
            BodyHandler bodyHandler = BodyHandler.create();
            StaticHandler staticHandler = StaticHandler.create("webroot");

            HealthChecks healthCheck = HealthChecks.create(vertx)
                .register("status-health", promise -> {
                    promise.complete(Status.OK());
                });
            HealthChecks clusterHealthCheck = HealthChecks.create(vertx)
                .register("cluster-health", ClusterHealthCheck.createProcedure(vertx));
                
            registeredRoutes.add("/index");
            registeredRoutes.add("/health");
            registeredRoutes.add("/readiness");
            registeredRoutes.add("/eventbus");

            mainRouter.route().handler(bodyHandler);
            mainRouter.get("/index").handler(staticHandler);
            // mainRouter.get("/health").handler(HealthCheckHandler.createWithHealthChecks(healthCheck));
            // mainRouter.get("/readiness").handler(HealthCheckHandler.createWithHealthChecks(clusterHealthCheck));

            addRoutes(mainRouter, config);

            routerPromise.complete(mainRouter);
        } catch ( Exception e ){
            routerPromise.fail(e);
        }
        return routerPromise.future();
    }
  
    private void addRoutes(Router mainRouter, JsonObject config) throws Exception {
        // Set<Class<?>> classes = getClassWithAnnotation(DmsController.class, config);
        Set<Class<? extends DmsAbstractBase>> classes = getClasses(DmsAbstractBase.class, config);

        Router restRouer = Router.router(vertx);
        Router eventBusRouer = Router.router(vertx);
        
        // 어노테이션을 적용받은 클래스들을 찾아 라우팅 설정
        for (Class<?> cls : classes) {
            // Class Annotation
            if (cls.isAnnotationPresent(DmsController.class)) {
                createController(restRouer, cls, config);
            } else if(cls.isAnnotationPresent(DmsSubscribe.class)){
                createEventBus(eventBusRouer, cls, config);
            }
        }
        mainRouter.route("/*").subRouter(restRouer);
        mainRouter.route("/eventbus/*").subRouter(eventBusRouer);
    }

    /**
     * Route 추가
     * @param router
     * @param path
     * @param httpMethods
     * @param requestHandler
     * @param faiHandler
     */
    private void addRoute(Router router, String path, Set<HttpMethod> httpMethods, Handler<RoutingContext> requestHandler, Handler<RoutingContext> faiHandler){
        // Vertx Route 생성
        Route route = router.route(path);
        for(HttpMethod httpMethod : httpMethods){
            route.method(httpMethod);
        }
        route.handler(CorsHandler.create().addOrigin("*").allowedMethods(httpMethods));
        route.handler(requestHandler);
        route.failureHandler(faiHandler);
        
        // 등록된 경로 로그 출력
        log.info("Route: http://{}:{}{}, Allow: {}", this.httpHost, this.httpPort, path, httpMethods);
    }
  
    private <T> Set<Class<? extends T>> getClasses(Class<T> subClass, JsonObject config) {
        Reflections reflections = new Reflections(config.getString("service.class", "com.dms.apps.vertx"));
        Set<Class<? extends T>> classes = reflections.getSubTypesOf(subClass);
        return classes;
    }

    private Set<Class<?>> getClassWithAnnotation(Class<? extends Annotation> annotationClass, JsonObject config){
        Reflections reflections = new Reflections(config.getString("service.class", "com.dms.apps.vertx"));
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(annotationClass);
        return classes;
    }

    private void injectFields(Object instance, Field[] fields){
        for (Field field : fields) {
            if (field.isAnnotationPresent(DmsInject.class)) {
                field.setAccessible(true); // private

                Class<?> fieldClass = field.getType();
                try {
                    if( fieldClass.equals(DmsDBClient.class) ){
                        field.set(instance, this.dmsDBClient);
                    } else if ( fieldClass.equals(DmsRedisClient.class) ){
                        field.set(instance, this.dmsRedisClient);
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Rest Route 처리
     * @param router
     * @param clazz
     * @param config
     * @throws Exception
     */
    private void createController(Router router, Class<?> clazz, JsonObject config) throws Exception {
        // Class Annotation 데이터 추출
        DmsController annotation = clazz.getAnnotation(DmsController.class);
        final String path = annotation.value();

        // Verticle 배포
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        DmsAbstractVerticle verticleInstance = (DmsAbstractVerticle) clazz.getDeclaredConstructor().newInstance();

        // Field Annotation
        Field[] fields = clazz.getDeclaredFields();
        injectFields(verticleInstance, fields);

        // Method Annotation
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(DmsRequestMapping.class)) {
                // Method Annotation 처리
                DmsRequestMapping mappingAnnotation = method.getAnnotation(DmsRequestMapping.class);
                
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

                // Router 추가
                addRoute(router, fullPath, httpMethods, ctx -> {
                    try {
                        log.info("[{}] {}", ctx.request().method().name(), ctx.request().path());
                        method.invoke(verticleInstance, ctx);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, ctx -> {
                    ctx.response().end(ctx.failure().getMessage());
                });
            }
        }

        // Verticle 배포
        vertx.deployVerticle(verticleInstance, options);
    }

    /**
     * EventBus Route 처리
     * @param router
     * @param clazz
     * @param config
     * @throws Exception
     */
    private void createEventBus(Router router, Class<?> clazz, JsonObject config) throws Exception {
        // Class Annotation 데이터 추출
        DmsSubscribe annotation = clazz.getAnnotation(DmsSubscribe.class);
        final String message = annotation.value().isBlank() ? annotation.message() : annotation.value();
        final int workers = annotation.workers();

        // Verticle Instance 생성
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        DmsAbstractWorker workerInstance = (DmsAbstractWorker) clazz.getDeclaredConstructor().newInstance();
        workerInstance.setMessage(message);
        workerInstance.setWorkers(workers);

        // Field Annotation Inject
        Field[] fields = clazz.getDeclaredFields();
        injectFields(workerInstance, fields);

        // -> Other Process Here
        SockJSHandlerOptions sockJSHandlerOptions = new SockJSHandlerOptions()
            .setHeartbeatInterval(2000);

        SockJSBridgeOptions bridgeOptions = new SockJSBridgeOptions()
            .addInboundPermitted(new PermittedOptions().setAddress(message))
            .addOutboundPermitted(new PermittedOptions().setAddress(message));
        
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, sockJSHandlerOptions);

        // router.route("/*").subRouter(sockJSHandler.bridge(bridgeOptions));
        router.route("/*").subRouter(sockJSHandler.socketHandler(sockJSSocket -> {
            // Retrieve the writeHandlerID and store it (e.g. in a local map)
            String writeHandlerID = sockJSSocket.writeHandlerID();
            log.info("writeHandlerID: {}", writeHandlerID);
            sockJSSocket.write("ping");

            // Just echo the data back
            // sockJSSocket.handler(sockJSSocket::write);
        }));

        // <- Other Process Here
        log.info("Route: http://{}:{}{}, Allow: {}", this.httpHost, this.httpPort, "/eventbus", message);

        // Verticle 배포
        vertx.deployVerticle(workerInstance, options);
    }

}
