-module(ai_postgres_join_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query } =  Ctx)->
  build(Query,Ctx).
build(#ai_db_query{join = undefined},Ctx)-> Ctx;
build(#ai_db_query{join = []},Ctx) -> Ctx;
build(#ai_db_query{join = Joins,table = Table},
      #ai_db_query_context{sql = Sql} =Ctx) ->
  Joins0 =
    lists:foldr(
      fun({Type,JoinTable,PK,FK},Acc)->
          Type0 = join_type(Type),
          JoinTable0 = ai_postgres_escape:escape_field(JoinTable),
          PK0 = key(JoinTable,PK),
          FK0 = key(Table,FK),
          <<Acc/binary,Type0/binary,JoinTable0/binary,
             " ON ",PK0/binary," = ",FK0/binary>>
      end,<<"">>,Joins),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary,Joins0/binary>>
   }.
  

join_type(inner) -> <<" INNER JOIN ">>;
join_type(left)  -> <<" LEFT JOIN ">>;
join_type(right) -> <<" RIGHT JOIN ">>;
join_type(full) -> <<" FULL JOIN ">>;
join_type(join) -> <<" JOIN ">>.

key({as,_Table,AsTable},Key)->
  ai_postgres_escape:escape_field({AsTable,Key});
key(Table,Key) ->
  ai_postgres_escape:escape_field({Table,Key}).
