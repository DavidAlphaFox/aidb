-module(ai_postgres_where_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  Ctx0 = build_where(Query,Ctx),
  build_extra(Query,Ctx0).
build_where(#ai_db_query{where = undefined},Ctx) -> Ctx;
build_where(#ai_db_query{where = []},Ctx)-> Ctx;
build_where(#ai_db_query{where = Where},
      #ai_db_query_context{
         sql = Sql,bindings = Bindings,
         options =Opts,slot = Slot } = Ctx)->
  {Values,Exprs,Count} =
    ai_postgres_condition_builder:transform(Where,Opts,{[], [], Slot}),
  WhereSql = ai_postgres_condition_builder:build(Exprs),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," WHERE ",WhereSql/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = Count
   }.

build_extra(#ai_db_query{extra_where = undefined},Ctx) -> Ctx;
build_extra(#ai_db_query{extra_where = []},Ctx)-> Ctx;
build_extra(#ai_db_query{extra_where = Where},
            #ai_db_query_context{ sql = Sql} = Ctx)->
  Exprs =
    lists:foldl(fun transform/2,<<"">>,Where),
  Ctx#ai_db_query_context{sql = <<Sql/binary,Exprs/binary>>}.

transform({Op,Cond},Acc)->
  Op0 =
    case Op of
      'and' -> <<" AND">>;
      'or' -> <<" OR">>;
      _ -> error({unsupported,operator, Op})
    end,
  <<Acc/binary,Op0/binary," ( ",Cond/binary," ) ">>;
transform(Cond,Acc) ->
  <<Acc/binary," AND ( ",Cond/binary," ) ">>.
