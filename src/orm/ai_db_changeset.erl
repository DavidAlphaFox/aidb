%%%-------------------------------------------------------------------
%%% @doc
%%% Based on Elixir `Ecto.Changeset'.
%%%
%%% Changesets allow filtering, casting, validation and definition of
%%% constraints when manipulating sumo models.
%%%
%%% @reference See
%%% <a href="https://hexdocs.pm/ecto/Ecto.Changeset.html">Ecto.Changeset</a>
%%% @end
%%% @end
%%%-------------------------------------------------------------------
-module(ai_db_changeset).

%% Properties
-export([
  model/1,
  errors/1,
  is_valid/1,
  required/1
]).

%% API
-export([
         add_error/3, add_error/4,
         apply_changes/1,
         cast/3, cast/4,
         validate/1,
         validate_change/3
        ]).

%%%=============================================================================
%%% Properties
%%%=============================================================================

model(#{model := Value}) -> Value.
errors(#{errors := Value}) -> Value.
changes(#{changes := Value}) -> Value.
is_valid(#{is_valid := Value}) -> Value.
types(#{types := Value}) -> Value.
required(#{required := Value}) -> Value.

%%%=============================================================================
%%% API
%%%=============================================================================

add_error(Changeset, Key, Message) -> add_error(Changeset, Key, Message, []).
add_error(#{errors := Errors} = Changeset, Key, Message, Keys) ->
  Changeset#{errors := [{Key, {Message, Keys}} | Errors], is_valid := false}.

%% 应用changest到当前模型上
apply_changes(#{changes := Changes, data := Data}) when map_size(Changes) == 0 -> Data;
apply_changes(#{changes := Changes, data := Data, types := Types}) ->
  maps:fold(fun(K, V, Acc) ->
    case maps:find(K, Types) of
      {ok, _} -> ai_db_model:set_field(K, V, Acc);
      error   -> Acc
    end
  end, Data, Changes).

%% 对已经存在的changeset进行二次变更
cast(#{model := ModelName, store := Store, data := Data, types := Types} = CS, Params, Allowed) ->
  NewChangeset = do_cast({ModelName, Store, Data, Types}, Params, Allowed),
  cast_merge(CS, NewChangeset).
%% 给某个模型进行变更，构建changeset
cast(ModelName, Fields, Params, Allowed) ->
  Metadata = get_metadata(ai_db_model:new(ModelName, Fields)),
  do_cast(Metadata, Params, Allowed).


get_field(Changeset, Key) -> get_field(Changeset, Key, undefined).

get_field(#{changes := Changes, data := Data}, Key, Default) ->
  case maps:find(Key, Changes) of
    {ok, Value} -> Value;
    error ->
      case maps:find(Key, Data) of
        {ok, Value} -> Value;
        error       -> Default
      end
  end.

validate(#{changes := Changes, attrs := Attrs } = Changeset)->
  Keys = maps:keys(Changes),
  lists:foldl(fun(Key,Acc)->
                  case maps:get(Key,Attrs) of
                    [] -> Acc;
                    Attr -> validate(Key,Attr,Acc)
                  end
              end,Changeset,Keys).
validate(Field,Attr,Changeset)->
  lists:foldl(fun
                ({length,Opts},Acc) -> validate_length(Acc, Field, Opts);
                ({number,Opts},Acc) -> validate_number(Acc, Field, Opts);
                ({format,Format},Acc) -> validate_format(Acc, Field, Format);
                ({inclusion,Enum},Acc) -> validate_inclusion(Acc, Field, Enum);
                (not_null,Acc) -> validate_required(Acc, [Field]);
                (_,Acc) -> Acc
   end,Changeset,Attr).
%% 验证修改
validate_change(#{changes := Changes, errors := Errors} = Changeset, Field, Validator) ->
    _ = ensure_field_exists(Changeset, Field),
    %% 抓取变更的值
    Value = fetch(Field, Changes),
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
        []      -> Changeset;
        [_ | _] -> Changeset#{errors := NewErrors2 ++ Errors, is_valid := false}
    end.
%% verify not null
validate_required(#{required := Required, errors := Errors} = CS, Fields) ->
  NewErrors =
        [
         begin
           {F, {blank, [{validation, required}]}}
         end || F <- Fields, is_missing(CS, F),
                ensure_field_exists(CS, F),
                is_nil(fetch(F, Errors))
        ],

  case NewErrors of
      [] -> CS#{required := Fields ++ Required};
      _  -> CS#{required := Fields ++ Required, errors := NewErrors ++ Errors, is_valid := false}
  end.

validate_inclusion(Changeset, Field, Enum) ->
  validate_change(Changeset, Field,
                  fun(_, Value) ->
                      case lists:member(Value, Enum) of
                        true  -> [];
                        false -> [{Field, {invalid, [{validation, inclusion}]}}]
                      end
                  end).

validate_number(Changeset, Field, Opts) ->
    Fun =
        fun(TargetField, Value) ->
                [ begin
                    case maps:find(SpecKey, number_validators(TargetValue)) of
                      {ok, {SpecFun, Message}} ->
                        validate_number(TargetField, Value, Message, SpecFun, TargetValue);
                      error ->
                        error({badarg, SpecKey})
                    end
                  end || {SpecKey, TargetValue} <- Opts]
          end,
  validate_change(Changeset,Field,Fun).

validate_number(Field, Value, Message, SpecFun, TargetValue) ->
  case SpecFun(Value, TargetValue) of
    true  -> [];
    false -> [{Field, {Message, [{validation, number}]}}]
  end.


validate_length(Changeset, Field, Opts) ->
  validate_change(Changeset, Field, fun(_, Value) ->
    case do_validate_length(length_validators(), Opts, byte_size(Value), undefined) of
      undefined -> [];
      Message   -> [{Field, {Message, [{validation, length}]}}]
    end
  end).

do_validate_length([], _, _, Acc) ->  Acc;
do_validate_length([{Opt, Validator} | T], Opts, Length, Acc) ->
  case fetch(Opt, Opts) of
      undefined -> do_validate_length(T, Opts, Length, Acc);
      Value ->
          case Validator(Length, Value) of
              undefined -> do_validate_length(T, Opts, Length, Acc);
              Message -> Message
          end
  end.

validate_format(Changeset, Field, Format) ->
  validate_change(Changeset, Field, fun(_, Value) ->
    case re:run(Value, Format) of
      nomatch -> [{Field, {invalid, [{validation, format}]}}];
      _       -> []
    end
  end).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================
%% Params 是map
%% Allowed 是list
do_cast({ModelName, Store, Data, Types,Attrs}, Params, Allowed) ->
  NewParams = convert_params(Params),
  FilteredParams =
    case Allowed of
      [] -> NewParams;
      _ -> maps:with(Allowed, NewParams)
    end,
    %% 生成Changes，错误和是否验证
  {Changes, Errors, IsValid} = maps:fold(
                                 fun(ParamKey, ParamVal, Acc) ->
                                     process_param(ParamKey, ParamVal, Types, Acc)
                                 end, {#{}, [], true}, FilteredParams),
  %% 生成changeset
  (changeset())#{
                 model   := ModelName,
                 store    := Store,
                 data     := Data,
                 params   := FilteredParams,
                 changes  := Changes,
                 errors   := Errors,
                 is_valid := IsValid,
                 types    := Types,
                 attrs  := Attrs
                }.

changeset() ->
  #{model    => undefined,
    store    => undefined,
    data     => undefined,
    params   => undefined,
    errors   => [],
    changes  => #{},
    is_valid => true,
    types    => undefined,
    attrs => undefined,
    required => []}.

store_attr(ModelName)->
  Key = {ModelName,store},
  Fun = fun() -> ai_db_manager:store(ModelName) end,
  ai_process:get(Key,Fun).

get_metadata(Model) ->
  ModelName = ai_db_model:name(Model),
  Store = store_attr(ModelName),
  Data = ai_db_model:fields(Model),
  Module = ai_db_model:module(Model),
  {Types,Attrs} = fields_info(Module:schema()),
  {ModelName, Store, Data, Types,Attrs}.

fields_info(Schema) ->
  lists:foldl(fun(F, {T,A}) ->
                  Name = ai_db_field:name(F),
                  {
                   maps:put(Name, ai_db_field:type(F),T),
                   maps:put(Name, ai_db_field:attrs(F),A)
                  }
              end, {#{},#{}}, ai_db_schema:fields(Schema)).

convert_params(Params) ->
  maps:fold(fun
              (K, V, Acc) when is_binary(K) ->
                maps:put(erlang:binary_to_existing_atom(K,utf8), V, Acc);
              (K, _, Acc) when is_atom(K)   -> Acc
  end, Params, Params).

process_param(ParamKey, ParamValue, Types, {Changes, Errors, IsValid}) ->
  case cast_field(ParamKey, ParamValue, Types) of
    {ok, CastValue} ->
      {maps:put(ParamKey, CastValue, Changes), Errors, IsValid};
    {invalid, Type} ->
      {Changes, [{ParamKey, {invalid, [{type, Type}, {validation, cast}]}} | Errors], false};
    missing -> {Changes, Errors, IsValid}
  end.

cast_field(Key, Value, Types) ->
  case maps:get(Key, Types, error) of
    error -> missing;
    Type ->
      case ai_db_type:cast(Type, Value) of
        {ok, _} = Ok -> Ok;
        {error, _}   -> {invalid, Type}
      end
  end.

cast_merge(CS1, CS2) ->
  NewChanges = maps:merge(changes(CS1), changes(CS2)),
  NewErrors = lists:usort(errors(CS1) ++ errors(CS2)),
  NewIsValid = is_valid(CS1) and is_valid(CS2),
  NewTypes = types(CS1),
  NewRequired = lists:usort(required(CS1) ++ required(CS2)),
  NewParams = maps:merge(cs_params(CS1), cs_params(CS2)),

  CS1#{
    params   := NewParams,
    changes  := NewChanges,
    errors   := NewErrors,
    is_valid := NewIsValid,
    types    := NewTypes,
    required := NewRequired
  }.

cs_params(#{params := Params}) ->
  case Params of
      undefined -> #{};
      _ -> Params
  end.

is_missing(Changeset, Field) ->
  case get_field(Changeset, Field) of
    undefined -> true;
    _   -> false
  end.

%% 确保某个域是存在的
%% 不准许更改不存在的域
ensure_field_exists(#{types := Types}, Field) ->
  case maps:is_key(Field, Types) of
    true  -> true;
    false -> error({badarg, Field})
  end.
is_nil(null) -> true;
is_nil(undefined) -> true;
is_nil(_)         -> false.


number_validators(N) ->
  #{
    less_than   => {fun(X, Y) -> X < Y end,  {less_than,N}},
    greater_than  => {fun(X, Y) -> X > Y end, {greater_than,N}},
    less_than_or_equal_to  => {fun(X, Y) -> X =< Y end, {less_than_or_equal_to,N}},
    greater_than_or_equal_to => {fun(X, Y) -> X >= Y end, {greater_than_or_equal_to,N}},
    equal_to => {fun(X, Y) -> X == Y end, {equal_to,N}}
   }.

wrong_length(Value, Value) -> undefined;
wrong_length(_Length, Value) -> {wrong_length,Value}.

too_short(Length, Value) when Length >= Value ->  undefined;
too_short(_Length, Value) -> {too_short,Value}.

too_long(Length, Value) when Length =< Value -> undefined;
too_long(_Length, Value) -> {too_long,Value}.

length_validators() ->
  [{is, fun wrong_length/2}, {min, fun too_short/2}, {max, fun too_long/2}].


fetch(Key, Proplists) when is_list(Proplists) ->
  case lists:keyfind(Key, 1, Proplists) of
    {Key, Value} -> Value;
    _            -> undefined
  end;

fetch(Key, Map) when is_map(Map) -> maps:get(Key, Map, undefined).
