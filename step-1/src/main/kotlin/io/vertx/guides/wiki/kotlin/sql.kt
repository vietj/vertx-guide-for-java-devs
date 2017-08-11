package io.vertx.guides.wiki.kotlin

val SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)"
val SQL_GET_PAGE = "select Id, Content from Pages where Name = ?"
val SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)"
val SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?"
val SQL_ALL_PAGES = "select Name from Pages"
val SQL_DELETE_PAGE = "delete from Pages where Id = ?"
