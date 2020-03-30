-module(ai_postgres_order_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{order_by = undefined},Ctx) -> Ctx;
build(#ai_db_query{order_by = Order},
      #ai_db_query_context{ sql = Sql } = Ctx)->
  Fields = lists:foldl(fun build_field/2,[],Order),
  Fields0 = lists:reverse(Fields),
  Fields1 = ai_string:join(Fields0,<<",">>),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," ORDER BY ",Fields1/binary>>
   }.

build_field({Table,OrderList},Acc)
  when erlang:is_list(OrderList) ->
  lists:foldl(fun({F,Sort},Acc0)->
                  F0 = ai_postgres_escape:escape_field({Table,F}),
                  Short0 = sort(Sort),
                  [<<F0/binary," ",Short0/binary>>|Acc0]
              end, Acc,OrderList);
build_field({F,Sort},Acc) ->
  F0 = ai_postgres_escape:escape_field(F),
  Short0 = sort(Sort),
  [<<F0/binary," ",Short0/binary>>|Acc].

sort(asc) -> <<"ASC">>;
sort(desc) -> <<"DESC">>.
