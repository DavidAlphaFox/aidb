-module(ai_postgres_insert_builder).
-compile([{inline,[transform/1]}]).
-include("ai_db_query.hrl").
-export([build/1]).

build(Ctx)->
  lists:foldl(
    fun(Fun,Acc)-> Fun(Acc) end,
    Ctx#ai_db_query_context{sql = <<"INSERT INTO ">>},
    [
     fun build_table/1,
     fun build_fields/1,
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
  {Fields, Values} =
        lists:foldl(
          fun({K, V}, {Fs, Vs}) ->
                  {[ai_postgres_escape:escape_field(K)|Fs], [V|Vs]}
          end, {[], []}, FieldsAndValues),

  SlotsFun = fun(N) -> NBin = erlang:integer_to_binary(N), <<"$",NBin/binary>>  end,
  NextSlot = Slot + erlang:length(Values),
  InsertSlots = lists:map(SlotsFun, lists:seq(Slot,NextSlot-1)),
  FieldsCSV = ai_string:join(lists:reverse(Fields), <<" , ">>),
  InsertSlotsCSV = ai_string:join(InsertSlots, <<" , ">>),

  Ctx#ai_db_query_context{
    sql = <<Sql/binary," ( ", FieldsCSV/binary, " ) ",
            " VALUES "," ( ", InsertSlotsCSV/binary, " ) ">>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = NextSlot
   }.

transform(Fields)
  when erlang:is_map(Fields) ->
  maps:to_list(Fields);
transform(Fields) -> Fields.
