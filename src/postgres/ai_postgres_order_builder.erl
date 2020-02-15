-module(ai_postgres_order_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{order_by = undefined},Ctx) -> Ctx;
build(#ai_db_query{order_by = []},Ctx)-> Ctx;
build(#ai_db_query{table = Table,order_by = Order},
      #ai_db_query_context{ sql = Sql,options = Opts } = Ctx)->
  Fields = lists:foldl(
             fun(F,Acc) -> build_field(Table,F,Acc) end,
             [],Order),
  Fields0 = lists:reverse(Fields),
  Fields1 = ai_string:join(Fields0,<<",">>),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," ORDER BY ",Fields1/binary>>
   }.

build_field(MainTable,{F,Sort},Acc)
  when erlang:is_atom(F)->
  F0 =
    case MainTable of
      {as,_Table,Alias} ->
        ai_postgres_escape:escape_field({Alias,F});
      _ ->
        ai_postgres_escape:escape_field({MainTable,F})
    end,
  Short0 = ai_string:to_string(Sort),
  [<<F0/binary," ",Short0/binary>>|Acc];
build_field(_MainTable,{F,Sort},Acc) ->
  F0 = ai_postgres_escape:escape_field(F),
  Short0 = ai_string:to_string(Sort),
  [<<F0/binary," ",Short0/binary>>|Acc].
