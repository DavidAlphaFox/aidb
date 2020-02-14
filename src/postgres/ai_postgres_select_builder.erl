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
     fun ai_postgres_join_builder:build/1,
     fun ai_postgres_where_builder:build/1
    ]).
  
build_from(#ai_db_query_context{
                query = Query,sql = Sql
               } = Ctx)->
  TableName = ai_postgres_escape:escape_field(Query#ai_db_query.table),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," FROM ",TableName/binary>>
   }.

build_fields(#ai_db_query_context{
                query = Query,sql = Sql
               } = Ctx)->
  {SelectFields,Ctx1} =
    case Query#ai_db_query.fields of
      undefined -> {<<" * ">>,Ctx };
      [] -> {<<" * ">>,Ctx};
      Fields ->
        {Fields0,Ctx0} =
          lists:foldl(fun build_field/2,{[],Ctx},Fields),
        {ai_string:join(Fields0,<<",">>),Ctx0}
    end,
  Ctx1#ai_db_query_context{
    sql = <<Sql/binary,SelectFields/binary>>
   }.

build_field({subquery,SubQuery,Alias},{F,Ctx})->
  Context =
    #ai_db_query_context{
       query = SubQuery,
       options = Ctx#ai_db_query_context.options,
       sql = <<"">>,
       bindings = [],
       slot = Ctx#ai_db_query_context.slot
      },
  {Slot,Bindings,Sql} =
    ai_postgres_subquery_builder:build(Context),
  SqlField = ai_postgres_escape:escape_field({sql,as,Sql,Alias}),
  {
   [SqlField|F],
   Ctx#ai_db_query_context{
     slot = Slot,
     bindings = Ctx#ai_db_query_context.bindings ++ Bindings
    }};

build_field({Table,TableFields},Acc)->
  lists:foldl(
    fun(TableField,{F,Ctx})->
        SqlField =
          case TableField of
            {sql,raw,_}->
              ai_postgres_escape:escape_field(TableField);
            {sql,as,_,_} ->
              ai_postgres_escape:escape_field(TableField);
            _ ->
              ai_postgres_escape:escape_field({Table,TableField})
          end,
        {[ SqlField| F],Ctx}
    end,Acc,TableFields).

