-module(ai_postgres_group_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{group_by = undefined},Ctx) -> Ctx;
build(#ai_db_query{group_by = []},Ctx)-> Ctx;
build(#ai_db_query{table = Table,group_by = Group},
      #ai_db_query_context{ sql = Sql } = Ctx)->
  Fields = lists:foldl(
             fun(F,Acc) -> build_field(Table,F,Acc) end,
             [],Group),
  Fields0 = lists:reverse(Fields),
  Fields1 = ai_string:join(Fields0,<<",">>),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," GROUP BY ",Fields1/binary>>
   }.

build_field(MainTable,F,Acc)
  when erlang:is_atom(F)->
  case MainTable of
    {as,_Table,Alias} ->
      [ai_postgres_escape:escape_field({Alias,F})|Acc];
    _ ->
      [ai_postgres_escape:escape_field({MainTable,F})|Acc]
  end;
build_field(_MainTable,F,Acc) ->
  [ai_postgres_escape:escape_field(F)|Acc].
