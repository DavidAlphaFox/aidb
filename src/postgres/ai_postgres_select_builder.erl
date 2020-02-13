-module(ai_postgres_select_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(Ctx)->
  lists:foldl(
    fun(Fun,Acc)-> Fun(Acc) end,
    Ctx#ai_db_query_context{sql = <<"SELECT ">>},
    [
         fun build_fields/1,
         fun build_from/1,
         fun ai_postgres_join_builder:build/1
       ]).
  
build_from(#ai_db_query_context{
                query = Query,sql = Sql
               } = Ctx)->
  TableName = ai_postgres_escape:escape_field(Query#ai_db_query.table),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," From ",TableName/binary>>
   }.

build_fields(#ai_db_query_context{
                query = Query,sql = Sql
               } = Ctx)->
  SelectFields =
    case Query#ai_db_query.fields of
      undefined -> <<" * ">>;
      [] -> <<" * ">>;
      Fields ->
        Fields0 =
          lists:foldl(
            fun(Item,Acc)->  build_field(Item,Acc) end,
            [],Fields),
        ai_string:join(Fields0,<<",">>)
    end,
  Ctx#ai_db_query_context{
    sql = <<Sql/binary,SelectFields/binary>>
   }.

build_field({Table,TableFields},Acc)->
  lists:foldl(
    fun(TableField,Acc0)->
        Field = ai_postgres_escape:escape_field({prefix,Table,TableField}),
        [ Field| Acc0]
    end,Acc,TableFields).
