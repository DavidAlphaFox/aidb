-module(ai_db_validator).
-export([validate/2]).

-spec validate(Fields::map(),
               Row::map()) -> ok | {error,list()}.
validate(Fields,Row)->
  Errors =
    maps:fold(
      fun(Key,Attrs,Acc)->
          case validate_null(Acc, Key, Attrs, Row) of
            {next,NewAcc}->
              validate(Key,maps:get(Key,Row,undefiend),Attrs,NewAcc);
            {stop,NewAcc}-> NewAcc
          end
       end,[],Fields),
  case Errors of
    [] -> ok;
    _ -> {error,Errors}
  end.

validate(Field,Value,Attrs,Acc)->
  case validate(Field,Value,Attrs) of
    [] -> Acc;
    Errors -> [{Field,Errors}|Acc]
  end.
validate(Field,Value,Attrs)->
  lists:foldl(
    fun
      ({length,Opts},Acc) -> validate_length(Acc,Field,Value, Opts);
      ({number,Opts},Acc) -> validate_number(Acc,Field,Value, Opts);
      ({format,Format},Acc) -> validate_format(Acc,Field,Value, Format);
      ({inclusion,Enum},Acc) -> validate_inclusion(Acc,Field,Value,Enum);
      (_,Acc) -> Acc
    end,[],Attrs).
  
validate_null(Errors,Key,Attrs,Row)->
  case proplists:lookup(not_null,Attrs) of
    {not_null,_}->
      case maps:is_key(Key,Row) of
        true -> {next,Errors};
        _ -> {stop,
              [{Key,[{blank, [{validation, required}]}]} |
              Errors]
             }
      end;
    _ -> {next,Errors}
  end.


validate_format(Errors,Field, Value, Format) ->
  Fun =
    fun(_, Value0) ->
        case re:run(Value0, Format) of
          nomatch -> [{Field, {invalid, [{validation, format}]}}];
          _       -> []
        end
    end,
  do_validate(Errors, Field,Value,Fun).

validate_inclusion(Errors, Field,Value,Enum) ->
  Fun =
    fun(_, Value0) ->
        case lists:member(Value0, Enum) of
          true  -> [];
          false -> [{Field, {invalid, [{validation, inclusion}]}}]
        end
    end,
  do_validate(Errors, Field,Value,Fun).
           

validate_number(Errors, Field,Value,Opts) ->
  Fun =
    fun(TargetField, Value0) ->
        [ begin
            case maps:find(SpecKey, number_validators(TargetValue)) of
              {ok, {SpecFun, Message}} ->
                validate_number(TargetField, Value0, Message,
                                SpecFun, TargetValue);
              error ->
                error({badarg, SpecKey})
            end
          end || {SpecKey, TargetValue} <- Opts]
    end,
  do_validate(Errors,Field,Value,Fun).

validate_number(Field, Value, Message, SpecFun, TargetValue) ->
  case SpecFun(Value, TargetValue) of
    true  -> [];
    false -> [{Field, {Message, [{validation, number}]}}]
  end.

validate_length(Errors, Field,Value,Opts) ->
  Fun =
    fun(_, Value0) ->
        case validate_length(
               length_validators(),
               Value0, Opts,
               byte_size(Value0),
               undefined) of
          undefined -> [];
          Message   -> [{Field, {Message, [{validation, length}]}}]
        end
    end,
  do_validate(Errors, Field,Value,Fun).

validate_length([], _, _, _, Acc) ->  Acc;
validate_length([{Opt, Validator} | T],Value, Opts, Length, Acc) ->
  case fetch(Opt,Opts) of
    undefined -> validate_length(T,Value, Opts, Length, Acc);
    Value ->
      case Validator(Length, Value) of
        undefined -> validate_length(T,Value, Opts, Length, Acc);
        Message -> Message
      end
  end.


number_validators(N) ->
  #{less_than   => {fun(X, Y) -> X < Y end,{less_than,N}},
    greater_than  => {fun(X, Y) -> X > Y end,{greater_than,N}},
    less_than_or_equal_to  => {fun(X, Y) -> X =< Y end,{less_than_or_equal_to,N}},
    greater_than_or_equal_to => {fun(X, Y) -> X >= Y end,{greater_than_or_equal_to,N}},
    equal_to => {fun(X, Y) -> X == Y end,{equal_to,N}}}.

is_length(Value, Value) -> undefined;
is_length(_Length, Value) -> {wrong_length,Value}.

too_short(Length, Value) when Length >= Value ->  undefined;
too_short(_Length, Value) -> {too_short,Value}.

too_long(Length, Value) when Length =< Value -> undefined;
too_long(_Length, Value) -> {too_long,Value}.

length_validators() ->
  [{is, fun is_length/2},
   {min, fun too_short/2},
   {max, fun too_long/2}].




do_validate(Errors,Field,Value, Validator) ->
    %% 抓取变更的值
  NewErrors1 =
    case is_nil(Value) of
      true  -> [];
      false -> Validator(Field, Value)
    end,
  %% 标准化错误结果
  NewErrors2 =
    [
     begin
       case Error of
         {K, V} when is_atom(K) -> {K, {V, []}};
         {K, {V, Opts}} when is_atom(K), is_list(Opts) -> {K, {V, Opts}}
       end
     end || Error <- NewErrors1],

  case NewErrors2 of
    []      -> Errors;
    [_ | _] -> NewErrors2 ++ Errors
  end.

is_nil(null) -> true;
is_nil(undefined) -> true;
is_nil(_) -> false.

fetch(Key, Proplists)->
  case lists:keyfind(Key, 1, Proplists) of
    {Key, Value} -> Value;
    _            -> undefined
  end.
