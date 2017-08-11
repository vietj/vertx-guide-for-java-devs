package io.vertx.guides.wiki.kotlin

import com.github.rjeschke.txtmark.Processor
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.UpdateResult
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.asyncResult
import io.vertx.kotlin.coroutines.runCoroutine
import java.util.*
import java.util.stream.Collectors

fun main(args : Array<String>) {
  val vertx = Vertx.vertx();
  vertx.deployVerticle(Server2());
}

class Server2 : CoroutineVerticle() {

  private val templateEngine = FreeMarkerTemplateEngine.create()

  suspend override fun start() {
    val dbClient = prepareDatabase()
    startHttpServer(dbClient)
    println("started")
  }

  private suspend fun startHttpServer(dbClient : JDBCClient) {
    val server = vertx.createHttpServer()

    val router = Router.router(vertx)

    router.get("/").handler { ctx -> indexHandler(ctx, dbClient) }
    router.get("/wiki/:page").handler { ctx -> indexHandler(ctx, dbClient) }
    router.get("/wiki/:page").handler { ctx -> pageRenderingHandler(ctx, dbClient) }
    router.post().handler(BodyHandler.create())
    router.post("/save").handler { ctx -> vertx.runCoroutine { pageUpdateHandler(ctx, dbClient) } }
//    router.post("/create").handler(Handler<RoutingContext> { this.pageCreateHandler(it) })
//    router.post("/delete").handler(Handler<RoutingContext> { this.pageDeletionHandler(it) })

    asyncResult<HttpServer> { ar ->
      server.requestHandler(/*Handler<HttpServerRequest> { router.accept(it) }*/ {
        req -> req.response().end("Hello World")
      }).listen(8080, ar)
    }
  }

  private fun indexHandler(context: RoutingContext, dbClient : JDBCClient) {

    vertx.runCoroutine {

      val connection = asyncResult<SQLConnection> { ar -> dbClient.getConnection(ar) }

      try {
        val pages = asyncResult<ResultSet> { ar -> connection.query(SQL_ALL_PAGES, ar) }
          .getResults()
          .stream()
          .map({ json -> json.getString(0) })
          .sorted()
          .collect(Collectors.toList<Any>())

        context.put("title", "Wiki home")  // <2>
        context.put("pages", pages)

        val markup = asyncResult<Buffer> { ar ->
          templateEngine.render(context, "templates", "/index.ftl", ar);
        }

        context.response().putHeader("Content-Type", "text/html").end(markup)

      } catch(e: Exception) {
        context.fail(e)
      } finally {
        connection.close()
      }
    }
  }

  private fun pageRenderingHandler(context: RoutingContext, dbClient : JDBCClient) {

    vertx.runCoroutine {

      val page = context.request().getParam("page")

      try {
        val connection = asyncResult<SQLConnection> { ar -> dbClient.getConnection(ar) }

        try {
          val result = asyncResult<ResultSet> { ar -> connection.queryWithParams(SQL_GET_PAGE, JsonArray().add(page), ar) }

          val row = result.getResults()
            .stream()
            .findFirst()
            .orElseGet({ JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN) })
          val id = row.getInteger(0)
          val rawContent = row.getString(1)

          context.put("title", page)
          context.put("id", id)
          context.put("newPage", if (result.getResults().size == 0) "yes" else "no")
          context.put("rawContent", rawContent)
          context.put("content", Processor.process(rawContent))  // <3>
          context.put("timestamp", Date().toString())

          val markup = asyncResult<Buffer> { ar -> templateEngine.render(context, "templates", "/page.ftl", ar) }
          context.response().putHeader("Content-Type", "text/html")
          context.response().end(markup)
        } finally {
          connection.close();
        }
      } catch(e: Exception) {
        context.fail(e)
      }
    }
  }

  private suspend fun pageUpdateHandler(context: RoutingContext, dbClient : JDBCClient) {
    val id = context.request().getParam("id")   // <1>
    val title = context.request().getParam("title")
    val markdown = context.request().getParam("markdown")
    val newPage = "yes" == context.request().getParam("newPage")  // <2>

    try {
      val connection = asyncResult<SQLConnection> { ar -> dbClient.getConnection(ar) }
      try {
        val sql : String
        val params = JsonArray()
        if (newPage) {
          sql = SQL_CREATE_PAGE;
          params.add(title).add(markdown)
        } else {
          sql = SQL_SAVE_PAGE
          params.add(markdown).add(id)
        }
        asyncResult<UpdateResult> { ar -> connection.updateWithParams(sql, params, ar) }
        context.response().statusCode = 303
        context.response().putHeader("Location", "/wiki/" + title)
        context.response().end()
      } finally {
        connection.close()
      }
    } catch(e: Exception) {
      context.fail(e)
    }
  }


  private suspend fun prepareDatabase(): JDBCClient {
    val dbClient = JDBCClient.createShared(vertx, JsonObject()
      .put("url", "jdbc:hsqldb:mem:wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30))
    val connection = asyncResult<SQLConnection> { ar ->
      dbClient.getConnection(ar)
    }
    try {
      asyncResult<Void> { ar ->
        connection.execute(SQL_CREATE_PAGES_TABLE, ar)
      }
      return dbClient
    } finally {
      connection.close()
    }
  }
}
