package io.vertx.guides.wiki.kotlin

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient

fun main(args : Array<String>) {
  val vertx = Vertx.vertx();
  vertx.deployVerticle(Server());
}

class Server : AbstractVerticle() {

  override fun start(startFuture: Future<Void>) {
    val steps = prepareDatabase()
      .compose ({ v -> startHttpServer() })
      .mapEmpty<Void>().setHandler(startFuture)
    println("started???")
  }


  private fun startHttpServer(): Future<HttpServer> {
    val future = Future.future<HttpServer>()
    val server = vertx.createHttpServer()

//    val router = Router.router(vertx)   // <2>
//    router.get("/").handler(Handler<RoutingContext> { this.indexHandler(it) })
//    router.get("/wiki/:page").handler(Handler<RoutingContext> { this.pageRenderingHandler(it) }) // <3>
//    router.post().handler(BodyHandler.create())  // <4>
//    router.post("/save").handler(Handler<RoutingContext> { this.pageUpdateHandler(it) })
//    router.post("/create").handler(Handler<RoutingContext> { this.pageCreateHandler(it) })
//    router.post("/delete").handler(Handler<RoutingContext> { this.pageDeletionHandler(it) })

    server.requestHandler(/*Handler<HttpServerRequest> { router.accept(it) }*/ {
      req -> req.response().end("Hello World")
    }).listen(8080, future)
    return future
  }

  private fun prepareDatabase(): Future<JDBCClient> {
    val future = Future.future<JDBCClient>()

    val dbClient = JDBCClient.createShared(vertx, JsonObject()
      .put("url", "jdbc:hsqldb:mem:wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30))

    dbClient.getConnection({
      ar ->
      if (ar.failed()) {
        future.fail(ar.cause())
      } else {
        val connection = ar.result()
        connection.execute(SQL_CREATE_PAGES_TABLE, { create ->
          connection.close()
          if (create.failed()) {
            future.fail(create.cause())
          } else {
            future.complete(dbClient)
          }
        })
      }
    })

    return future
  }
}
