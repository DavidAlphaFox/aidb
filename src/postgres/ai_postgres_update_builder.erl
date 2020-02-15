-module(ai_postgres_update_builder).
-compile([{inline,[transform/1]}]).
-include("ai_db_query.hrl").
-export([build/1]).

build(Ctx)->
  lists:foldl(
    fun(Fun,Acc)-> Fun(Acc) end,
    Ctx#ai_db_query_context{sql = <<"UPDATE ">>},
    [
     fun build_table/1,
     fun build_fields/1,
     fun ai_postgres_where_builder:build/1,
     fun ai_postgres_returning_builder:build/1
    ]).
build_table(#ai_db_query_context{
               query = Query,
               sql = Sql
              } = Ctx)->
  Table0 =
    case Query#ai_db_query.table of
      {as,Table,_Alias}-> ai_postgres_escape:escape_field(Table);
      Table -> ai_postgres_escape:escape_field(Table)
    end,
  Ctx#ai_db_query_context{
    sql = <<Sql/binary, Table0/binary>>
   }.
build_fields(#ai_db_query_context{
                query = Query,sql = Sql,
                slot = Slot,bindings = Bindings
               } = Ctx)->
  FieldsAndValues = transform(Query#ai_db_query.fields),
  {NextSlot,Fields, Values} =
    lists:foldl(
      fun({K, V}, {Index,Fs, Vs}) ->
          IndexBin = erlang:integer_to_binary(Index),
          FieldName = ai_postgres_escape:escape_field(K),
          UpdateSlot = <<FieldName/binary, " = $", IndexBin/binary>>,
          {Index+1,[UpdateSlot|Fs],[V|Vs]}
      end, {Slot,[], []}, FieldsAndValues),
  UpdateSlotsCSV = ai_string:join(lists:reverse(Fields), <<",">>),

  Ctx#ai_db_query_context{
    sql = <<Sql/binary," SET ",UpdateSlotsCSV/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = NextSlot
   }.

transform(Fields)
  when erlang:is_map(Fields) ->
  maps:to_list(Fields);
transform(Fields) -> Fields.
