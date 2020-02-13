-module(ai_postgres_subquery_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(Ctx)->
  Ctx0 = ai_postgres_select_builder:build(Ctx),
  Sql = Ctx0#ai_db_query_context.sql,
  Sql0 = <<"( ", Sql/binary, " )">>,
  {
   Ctx0#ai_db_query_context.slot,
   Ctx0#ai_db_query_context.bindings,
   Sql0
  }.
