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
     fun ai_postgres_where_builder:build/1,
     fun ai_postgres_group_builder:build/1,
     fun ai_postgres_having_builder:build/1,
     fun ai_postgres_order_builder:build/1,
     fun ai_postgres_limit_builder:build/1
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
        SqlField = escape_field(Table,TableField),
        {[ SqlField| F],Ctx}
    end,Acc,TableFields);
build_field(TableFields,{F,Ctx} = Acc)
  when erlang:is_list(TableFields)->
  Query = Ctx#ai_db_query_context.query,
  Opts = Ctx#ai_db_query_context.options,
  TableName =
    case Query#ai_db_query.table of
      {as,_,Alias} -> Alias;
      Table -> Table
    end,
  NeedPrefix = proplists:get_value(prefix,Opts,false),
  if
    NeedPrefix == true -> build_field({TableName,TableFields},Acc);
    true ->
      F1 =
        lists:foldl(
          fun(TableField,F0)->
              SqlField = ai_postgres_escape:escape_field(TableField),
              [ SqlField| F0]
          end,F,TableFields),
      {F1,Ctx}
  end;
build_field(TableField,{F,Ctx}) ->
  Query = Ctx#ai_db_query_context.query,
  Opts = Ctx#ai_db_query_context.options,
  TableName =
    case Query#ai_db_query.table of
      {as,_,Alias} -> Alias;
      Table -> Table
    end,
  NeedPrefix = proplists:get_value(prefix,Opts,false),
  if
    NeedPrefix == true ->
      {[escape_field(TableName,TableField)|F],Ctx};
    true ->
      SqlField = ai_postgres_escape:escape_field(TableField),
      {[SqlField|F],Ctx}
  end.

escape_field(_Table,{sql,raw,_} = TableField)-> ai_postgres_escape:escape_field(TableField);
escape_field(_Table,{sql,as,_,_} = TableField)-> ai_postgres_escape:escape_field(TableField);
escape_field(Table,{proc,Proc,Field})-> ai_postgres_escape:escape_field({proc,Proc,{Table,Field}});
escape_field(Table,{proc,Proc,Field,Alias})->ai_postgres_escape:escape_field({proc,Proc,{Table,Field},Alias});
escape_field(Table,TableField)-> ai_postgres_escape:escape_field({Table,TableField}).
