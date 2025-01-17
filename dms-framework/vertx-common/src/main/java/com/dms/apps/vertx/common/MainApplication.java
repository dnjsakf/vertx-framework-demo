package com.dms.apps.vertx.common;

public class MainApplication {
  public static void main(String[] args) {
    new DmsVertxLauncher().dispatch(new String[]{
      "run", "java:"+VertxProcessor.class.getCanonicalName()
    });
    // Vertx vertx = Vertx.vertx();

    // ConfigStoreOptions jsonFileStore = new ConfigStoreOptions()
    //   .setType("file")
    //   .setFormat("json")
    //   .setConfig(new JsonObject().put("path", "properties/vertx-config.json"));

    // ConfigRetrieverOptions options = new ConfigRetrieverOptions()
    //   .addStore(jsonFileStore)
    // ;

    // ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
    // retriever.getConfig(ar -> {
    //   if( ar.succeeded() ){
    //     JsonObject config = ar.result();
    //     vertx.deployVerticle(new SampleVerticle(), config);
    //   } else {
        
    //   }
    // });
  } 
}
