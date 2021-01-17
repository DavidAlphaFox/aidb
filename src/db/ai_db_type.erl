-module(ai_db_type).

-export([is_datetime/1]).

-export([cast/2]).

is_datetime({_, _, _} = Date) -> calendar:valid_date(Date);
is_datetime({{_, _, _} = Date, {H, M, S}}) ->
  calendar:valid_date(Date) and (H >= 0 andalso H =< 23)
    and (M >= 0 andalso M =< 59)
    and (S >= 0 andalso S =< 59);
is_datetime(_) -> false.

cast(_, undefined) -> {ok, undefined};
cast(_, null) -> {ok, null};
cast(float, Data) when is_integer(Data) -> {ok, Data + 0.0};
cast(float, Data) when is_list(Data) orelse is_binary(Data) -> cast_float(Data);
cast(integer, Data) when is_float(Data) -> {ok, erlang:trunc(Data)};
cast(integer, Data) when is_list(Data) orelse is_binary(Data) -> cast_integer(Data);
cast(Type, Data)
  when is_list(Data), Type /= binary, Type /= custom ->
  cast(Type, ai_string:to_string(Data));
cast(string, Data)
  when is_binary(Data); is_atom(Data); is_number(Data) ->
  {ok, ai_string:to_string(Data)};
cast(boolean, Data) when is_binary(Data) ->
  BinData = string:lowercase(ai_string:to_string(Data)),
  case lists:member(BinData, [<<"true">>, <<"1">>,<<"t">>]) of
    true -> {ok, true};
    false ->
      case lists:member(BinData, [<<"false">>, <<"0">>,<<"f">>]) of
        true -> {ok, false};
        false -> {error, {invalid, Data}}
      end
  end;
cast(date, {_, _, _} = Data) -> cast(datetime, {Data, {0, 0, 0}});
cast(Type, Data)
    when erlang:is_binary(Data) andalso
         (Type == date orelse Type == datetime) ->
    try {ok, ai_iso8601:parse(Data)} catch
      _:_ -> {error, {invalid, Data}}
    end;
cast(Type, Data) ->
    Fun = maps:get(Type, primitives(), fun (_) -> false end),
    case Fun(Data) of
      true -> {ok, Data};
      false -> {error, {invalid, Data}}
    end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================


primitives() ->
  #{string => fun erlang:is_binary/1,
    integer => fun erlang:is_integer/1,
    float => fun erlang:is_float/1,
    boolean => fun erlang:is_boolean/1,
    date => fun is_datetime/1,
    datetime => fun is_datetime/1,
    binary => fun erlang:is_binary/1,
    custom => fun (_) -> true end}.

cast_float(Data) ->
  case string:to_float(Data) of
    {error, no_float} ->
      case cast_integer(Data) of
        {ok, Integer} -> {ok, Integer + 0.0};
        Error -> Error
      end;
    {Float, _Rest} -> {ok, Float}
  end.

%% @private
cast_integer(Data) ->
  case string:to_integer(Data) of
    {error, no_integer} -> {error, {invalid, Data}};
    {Integer, _Rest} -> {ok, Integer}
  end.
