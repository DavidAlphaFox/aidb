-module(ai_postgres_having_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).

build(#ai_db_query{group_by = undefined},_Ctx) -> error({missing,group_by});
build(#ai_db_query{group_by = []},_Ctx)-> error({missing,group_by});
build(#ai_db_query{having = undefined},Ctx)-> Ctx;
build(#ai_db_query{having = []},Ctx)-> Ctx;
build(#ai_db_query{having = Having},
      #ai_db_query_context{
         sql = Sql,bindings = Bindings,
         options =Opts,slot = Slot } = Ctx)->
  {Values,Exprs,Count} =
    ai_postgres_condation_builder:transform(Having,Opts,{[], [], Slot}),
  HavingSql = ai_postgres_condation_builder:build(Exprs),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," HAVING ",HavingSql/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = Count
   }.
