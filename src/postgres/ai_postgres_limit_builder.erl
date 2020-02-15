-module(ai_postgres_limit_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{limit = undefined, offset = undefined},Ctx) -> Ctx;
build(#ai_db_query{limit = Limit,offset = Offset},
      #ai_db_query_context{ sql = Sql,slot = Slot,
                            bindings = Bindings } = Ctx)->
  {Sql0,Slot0,Bindings0} =
    case Limit of
      0 -> {Sql,Slot,Bindings};
      undefined -> {Sql,Slot,Bindings};
    _ ->
      LimitSlot = erlang:integer_to_binary(Slot),
      {<<Sql/binary," LIMIT $",LimitSlot/binary>>,Slot + 1,Bindings ++ [Limit]}
    end,
  {Sql1,Slot1,Bindings1} =
    case Offset of
      0 -> {Sql0,Slot0,Bindings0};
      undefined -> {Sql0,Slot0,Bindings0};
      _ ->
        OffsetSlot = erlang:integer_to_binary(Slot0),
        {<<Sql0/binary," OFFSET $",OffsetSlot/binary>>,Slot0 + 1, Bindings0 ++ [Offset]}
    end,
  Ctx#ai_db_query_context{
    sql = Sql1,
    bindings = Bindings1,
    slot = Slot1
   }.
