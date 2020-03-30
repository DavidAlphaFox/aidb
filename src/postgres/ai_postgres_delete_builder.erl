-module(ai_postgres_delete_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(Ctx)->
  lists:foldl(
    fun(Fun,Acc)-> Fun(Acc) end,
    Ctx#ai_db_query_context{sql = <<"DELETE FROM ">>},
    [
     fun build_table/1,
     fun ai_postgres_where_builder:build/1
    ]).
build_table(#ai_db_query_context{
               query = Query,
               sql = Sql
              } = Ctx)->
  Table = ai_postgres_escape:escape_field(Query#ai_db_query.table),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary, Table/binary>>
   }.
