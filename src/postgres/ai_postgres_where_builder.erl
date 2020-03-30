-module(ai_postgres_where_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query,sql = Sql} = Ctx)->
  Where = Query#ai_db_query.where,
  ExtraWhere = Query#ai_db_query.extra_where,
  if
    Where == ExtraWhere -> Ctx;
    true -> build(Where,ExtraWhere,
                  Ctx#ai_db_query_context{sql = <<Sql/binary, " WHERE ">>})
  end.
build(undefined,ExtraWhere,
      #ai_db_query_context{sql = Sql } = Ctx) ->
  Ctx#ai_db_query_context{ sql = <<Sql/binary,ExtraWhere/binary>> };
build(Where,undefined,Ctx) -> build(Where,Ctx);
build(Where,{Op,ExtraWhere},Ctx) ->
  Ctx0 = build(Where,Ctx),
  Sql = Ctx0#ai_db_query_context.sql,
  Op0 =
    case Op of
      'and' -> <<" AND">>;
      'or' -> <<" OR">>;
      _ -> error({unsupported,operator, Op})
    end,
  Ctx0#ai_db_query_context{ sql = <<Sql/binary,Op0/binary, " ( ", ExtraWhere/binary," )">> };
build(Where,ExtraWhere,Ctx) ->
  Ctx0 = build(Where,Ctx),
  Sql = Ctx0#ai_db_query_context.sql,
  Ctx0#ai_db_query_context{ sql = <<Sql/binary," AND ( ", ExtraWhere/binary," )">> }.

build(Where,#ai_db_query_context{
               sql = Sql,bindings = Bindings,
               options = Opts,slot = Slot } = Ctx)->
  {Values,Exprs,Count} =
    ai_postgres_condition:transform(Where,Opts,{[], [], Slot}),
  WhereSql = ai_postgres_condition:build(Exprs),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary,WhereSql/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = Count
   }.
