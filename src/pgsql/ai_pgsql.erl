-module(ai_pgsql).
-export([
         build_insert/1,
         build_insert/2,
         build_update/1,
         build_update/2,
         build_select/1,
         build_result/1,
         build_result/2
        ]).
-spec build_insert([{term(),term()}])-> {binary(),[term()],integer()}.
build_insert(FieldsAndValues)-> build_insert(1,FieldsAndValues).

-spec build_insert(integer(),[{term(),term()}]) ->
        {binary(),[term()],integer()}.
build_insert(Slot,FieldsAndValues)->
  {Fields, Values} =
        lists:foldl(
          fun({K, V}, {Fs, Vs}) ->
              {[ai_pgsql_escape:field(K)|Fs], [null(V)|Vs]}
          end, {[], []}, FieldsAndValues),
  NextSlot = Slot + erlang:length(Values),
  InsertSlots = lists:map(fun ai_pgsql_escape:slot/1,
                          lists:seq(Slot,NextSlot-1)),
  FieldsCSV = ai_string:join(lists:reverse(Fields), <<" , ">>),
  InsertSlotsCSV = ai_string:join(InsertSlots, <<" , ">>),
  {
   <<" ( ", FieldsCSV/binary, " ) ",
     " VALUES "," ( ", InsertSlotsCSV/binary, " ) ">>,
   lists:reverse(Values),
   NextSlot
  }.

-spec build_update([{term(),term()}])-> {binary(),[term()],integer()}.
build_update(FieldsAndValues)-> build_update(1,FieldsAndValues).

-spec build_update(integer(),[{term(),term()}]) ->
        {binary(),[term()],integer()}.
build_update(Slot,FieldsAndValues)->
  {NextSlot,Fields, Values} =
    lists:foldl(
      fun({K, V}, {Index,Fs, Vs}) ->
          IndexBin = erlang:integer_to_binary(Index),
          FieldName = ai_pgsql_escape:field(K),
          UpdateSlot = <<FieldName/binary, " = $", IndexBin/binary>>,
          {Index+1,[UpdateSlot|Fs],[null(V)|Vs]}
      end, {Slot,[], []}, FieldsAndValues),
  UpdateSlotsCSV = ai_string:join(lists:reverse(Fields), <<",">>),
  {
   <<" SET ",UpdateSlotsCSV/binary>>,
   lists:reverse(Values),
   NextSlot
  }.

-spec build_select(Columns :: [ai_pgsql_escape:column()])->
        {ok,binary()}.
build_select(Columns)->
  Columns0 =
    lists:map(fun ai_pgsql_escape:field/1,Columns),
  {ok,ai_string:join(Columns0,<<",">>)}.

-spec build_result({ok,list(),list()} |
                   {ok,integer(),list(),list()}|
                   {ok,integer()}|
                   {error,term()} )->
        {error,not_persist} |
        {ok,integer()} |
        {ok,integer(),map()}|
        {ok,map()}.
build_result(Result) -> build_result(Result,undefined).

-spec build_result({ok,list(),list()} |
                   {ok,integer(),list(),list()}|
                   {ok,integer()}|
                   {error,term()},function()|undefined )->
        {error,not_persist} |
        {ok,integer()} |
        {ok,integer(),map()}|
        {ok,map()}.
build_result({ok,Cols,Rows},ColFun)->
  {ok,build_result(Cols,Rows,ColFun)};
build_result({ok,Count} = R,_ColFun)->
  if Count >= 1 -> R;
     true -> {error,not_persist}
  end;
build_result({ok,Count,Cols,Rows},ColFun)->
  if Count >= 1 ->
      {ok,Count,
       build_result(Cols,Rows,ColFun)};
     true -> {error,not_persist}
  end;
build_result(Error,_ColFun) -> Error.

build_result(Cols,Rows,undefined) ->
  ai_pgsql_row:to_map(Cols, Rows);
build_result(Cols,Rows,ColFun)->
  ai_pgsql_row:to_map(Cols, Rows, ColFun).

%%%===================================================================
%%% Internal functions
%%%===================================================================
null(undefined)-> null;
null(Other) -> Other.
