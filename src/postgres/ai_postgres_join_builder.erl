-module(ai_postgres_join_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query } =  Ctx)-> build(Query,Ctx).
build(#ai_db_query{join = undefined},Ctx)-> Ctx;
build(#ai_db_query{join = Joins,table = Table},Ctx) ->
  lists:foldl(
    fun(Join,Acc)-> build(Table,Join,Acc) end,
    Ctx,Joins).

build(Table,{Type,{query,SubQuery,Alias},PK,FK},Ctx)->
  Sql = Ctx#ai_db_query_context.sql,
  SubQueryContext =
    #ai_db_query_context{
       query = SubQuery,
       options = Ctx#ai_db_query_context.options,
       sql = <<"">>,
       bindings = [],
       slot = Ctx#ai_db_query_context.slot
      },
  {Slot,Bindings,SubQuerySql} = build_query(SubQueryContext,Alias),
  Type0 = join_type(Type),
  PK0 = key(Alias,PK),
  FK0 = key(Table,FK),

  Ctx#ai_db_query_context{
    slot = Slot,
    bindings = Ctx#ai_db_query_context.bindings ++ Bindings,
    sql = <<Sql/binary,Type0/binary,SubQuerySql/binary,
             " ON ",PK0/binary," = ",FK0/binary>>
   };
build(Table,{Type,JoinTable,PK,FK},
      #ai_db_query_context{sql = Sql} = Ctx)->
  Type0 = join_type(Type),
  JoinTable0 = ai_postgres_escape:escape_field(JoinTable),
  PK0 = key(JoinTable,PK),
  FK0 = key(Table,FK),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary,Type0/binary,JoinTable0/binary,
    " ON ",PK0/binary," = ",FK0/binary>>
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


build_query(Ctx,Alias)->
  Ctx0 = ai_postgres_select_builder:build(Ctx),
  Alias0 = ai_string:to_string(Alias),
  Sql = Ctx0#ai_db_query_context.sql,
  Sql0 = <<"( ", Sql/binary, " ) AS ",Alias0/binary>>,
  {
   Ctx0#ai_db_query_context.slot,
   Ctx0#ai_db_query_context.bindings,
   Sql0
  }.
