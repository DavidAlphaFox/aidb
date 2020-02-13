-module(ai_postgres_query_builder).
-include("ai_db_query.hrl").

-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query#ai_db_query.action,Ctx).

build(select,Ctx)-> ai_postgres_select_builder:build(Ctx).
