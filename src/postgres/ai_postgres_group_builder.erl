-module(ai_postgres_group_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{group_by = undefined},Ctx) -> Ctx;
build(#ai_db_query{group_by = Group},
      #ai_db_query_context{ sql = Sql } = Ctx)->
  Fields = lists:foldl(fun build_field/2,[],Group),
  Fields0 = lists:reverse(Fields),
  Fields1 = ai_string:join(Fields0,<<",">>),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," GROUP BY ",Fields1/binary>>
   }.

build_field({Table,GroupList},Acc)
  when erlang:is_list(GroupList) ->
  lists:foldl(
    fun(F,Acc0)->
        [ai_postgres_escape:escape_field({Table,F})|Acc0]
    end, Acc,GroupList);
build_field(F,Acc) ->
  [ai_postgres_escape:escape_field(F)|Acc].

