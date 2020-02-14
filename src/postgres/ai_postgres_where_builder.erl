-module(ai_postgres_where_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{where = undefined},Ctx) -> Ctx;
build(#ai_db_query{where = []},Ctx)-> Ctx;
build(#ai_db_query{where = Where},
      #ai_db_query_context{
         sql = Sql,bindings = Bindings,
         options =Opts,slot = Slot } = Ctx)->
  {Values,Exprs,Count} =
    ai_postgres_condation_builder:transform(Where,Opts,{[], [], Slot}),
  WhereSql = ai_postgres_condation_builder:build(Exprs),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," WHERE ",WhereSql/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = Count
   }.
