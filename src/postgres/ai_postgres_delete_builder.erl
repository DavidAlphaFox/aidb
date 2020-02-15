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
  Table0 =
    case Query#ai_db_query.table of
      {as,Table,_Alias}-> ai_postgres_escape:escape_field(Table);
      Table -> ai_postgres_escape:escape_field(Table)
    end,
  Ctx#ai_db_query_context{
    sql = <<Sql/binary, Table0/binary>>
   }.
