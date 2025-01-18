package com.dms.apps.vertx.common;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;

import com.dms.apps.vertx.common.abs.DmsAbstractVerticle;
import com.dms.apps.vertx.common.annotations.DmsVertxController;
import com.dms.apps.vertx.common.annotations.DmsVertxMapping;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
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

    private String host;
    private Integer port;

    public String getHost(){
        return this.host;
    }
    public Integer getPost(){
        return this.port;
    }
    
    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        DmsVertxLauncher.getConfig(ar -> {
            if( ar.succeeded() ){
                JsonObject config = ar.result();
                String host = config.getString("http.host", "localhost");
                Integer port = config.getInteger("http.port", 8080);
                
                this.host = host;
                this.port = port;

                try {
                    vertx.createHttpServer()
                        .requestHandler(createRouter(config))
                        .listen(port, host, http -> {
                            if (http.succeeded()) {
                                log.info("HTTP 서버가 시작되었습니다: http://"+host+":"+port);
                                startPromise.complete();
                            } else {
                                log.error(http.cause().getMessage(), http.cause());
                                startPromise.fail(http.cause());
                            }
                        });
                } catch ( Exception e ){
                    startPromise.fail(e);
                }
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }

    
    /**
     * 라우터 생성
     * @return
     */
    private Router createRouter(JsonObject config) throws Exception{
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        setDynamicRoutes(router, config);

        return router;
    }

    // Reflection을 사용하여 어노테이션이 적용된 클래스를 가져오는 메소드
    private Set<Class<?>> getClassesWithAnnotation(JsonObject config) throws Exception {
        Reflections reflections = new Reflections(config.getString("service.class", "com.dms.apps"));
        return reflections.getTypesAnnotatedWith(DmsVertxController.class);
    }

  
    /**
     * DmsAbstractVerticle이 적용된 클래스에서
     * DmsVertxMapping 어노테이션이 적용된 메소드를 찾아서
     * 라우팅 처리
     * @param router
     */
    private void setDynamicRoutes(Router router, JsonObject config) throws Exception {
        // 어노테이션을 적용받은 클래스들을 찾아 라우팅 설정
        for (Class<?> cls : getClassesWithAnnotation(config)) {
            if (cls.isAnnotationPresent(DmsVertxController.class)) {
                DmsVertxController annotation = cls.getAnnotation(DmsVertxController.class);
                final String path = annotation.value();

                DeploymentOptions options = new DeploymentOptions().setConfig(config);
                DmsAbstractVerticle verticleInstance = (DmsAbstractVerticle) cls.getDeclaredConstructor().newInstance();
                vertx.deployVerticle(verticleInstance, options);

                for (Method method : cls.getDeclaredMethods()) {
                    if (method.isAnnotationPresent(DmsVertxMapping.class)) {
                        DmsVertxMapping mappingAnnotation = method.getAnnotation(DmsVertxMapping.class);
                        String fullPath = path + mappingAnnotation.value();

                        if (registeredRoutes.contains(fullPath)) {
                            throw new IllegalArgumentException("중복 경로가 발견되었습니다: " + fullPath);
                        } else {
                            registeredRoutes.add(fullPath);
                        }

                        String[] mappingMethods = mappingAnnotation.methods();
                        if( mappingMethods == null || mappingMethods.length == 0 ){
                            mappingMethods = DEFAULT_METHODS;
                        }

                        Set<HttpMethod> httpMethods = (Set<HttpMethod>) Arrays.stream(mappingMethods)
                            .map(httpMethod -> { 
                                return new HttpMethod(httpMethod);
                            }).collect(Collectors.toSet());

                        log.info("Route: http://{}:{}{}, Allow: {}", this.host, this.port, fullPath, httpMethods);
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
