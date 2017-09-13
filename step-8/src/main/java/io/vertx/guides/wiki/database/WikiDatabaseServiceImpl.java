/*
 *  Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *  Copyright (c) 2017 INSA Lyon, CITI Laboratory.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.guides.wiki.database;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Single;

import java.util.HashMap;
import java.util.List;

/**
 * @author <a href="https://julien.ponge.org/">Julien Ponge</a>
 */
class WikiDatabaseServiceImpl implements WikiDatabaseService {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);

  private final HashMap<SqlQuery, String> sqlQueries;
  private final JDBCClient dbClient;

  WikiDatabaseServiceImpl(io.vertx.ext.jdbc.JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries, Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
    this.dbClient = new JDBCClient(dbClient);
    this.sqlQueries = sqlQueries;

    getConnection()
      .flatMap(conn -> conn.rxExecute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE)))
      .map(v -> this)
      .subscribe(RxHelper.toSubscriber(readyHandler));
  }

  // tag::rx-get-connection[]
  private Single<SQLConnection> getConnection() {
    return dbClient.rxGetConnection().flatMap(conn -> {
      Single<SQLConnection> connectionSingle = Single.just(conn); // <1>
      return connectionSingle.doOnUnsubscribe(conn::close); // <2>
    });
  }
  // end::rx-get-connection[]

  @Override
  // tag::rx-data-flow[]
  public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
    dbClient
      .rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES))
      .flatMapObservable(res -> {  // <1>
        List<JsonArray> results = res.getResults();
        return Observable.from(results); // <2>
      })
      .map(json -> json.getString(0)) // <3>
      .sorted() // <4>
      .collect(JsonArray::new, JsonArray::add) // <5>
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }
  // end::rx-data-flow[]

  @Override
  public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxQueryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(name)))
      .map(result -> {
        if (result.getNumRows() > 0) {
          JsonArray row = result.getResults().get(0);
          return new JsonObject()
            .put("found", true)
            .put("id", row.getInteger(0))
            .put("rawContent", row.getString(1));
        } else {
          return new JsonObject().put("found", false);
        }
      })
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService fetchPageById(int id, Handler<AsyncResult<JsonObject>> resultHandler) {
    Single<SQLConnection> connection = getConnection();
    // tag::rx-execute-query-with-params[]
    Single<ResultSet> resultSet = connection
      .flatMap(conn -> conn.rxQueryWithParams(sqlQueries.get(SqlQuery.GET_PAGE_BY_ID), new JsonArray().add(id)));
    // end::rx-execute-query-with-params[]
    resultSet
      .map(result -> {
        if (result.getNumRows() > 0) {
          JsonObject row = result.getRows().get(0);
          return new JsonObject()
            .put("found", true)
            .put("id", row.getInteger("ID"))
            .put("name", row.getString("NAME"))
            .put("content", row.getString("CONTENT"));
        } else {
          return new JsonObject().put("found", false);
        }
      })
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxUpdateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), new JsonArray().add(title).add(markdown)))
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxUpdateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), new JsonArray().add(markdown).add(id)))
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(connection -> {
        JsonArray data = new JsonArray().add(id);
        return connection.rxUpdateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data);
      })
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  // tag::rxhelper-to-subscriber[]
  public WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler) { // <1>
    getConnection()
      .flatMap(connection -> connection.rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES_DATA)))
      .map(ResultSet::getRows)
      .subscribe(RxHelper.toSubscriber(resultHandler));  // <2>
    return this;
  }
  // end::rxhelper-to-subscriber[]
}
