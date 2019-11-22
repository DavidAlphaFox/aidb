-module(ai_store_mnesia).

%% API
-export([
         init/1,
         persist/2,
         fetch/3,
         delete_by/3,
         delete_all/2,
         find_all/2,
         find_all/5,
         find_by/3,
         find_by/5,
         find_by/6,
         count/2,
         count_by/3,
         dirty/2,
         transaction/2
        ]).

-record(state,{
               verbose
              }).

%%%=============================================================================
%%% API
%%%=============================================================================
init(Options) ->
    Verbose = maps:get(verbose, Options,false),
    {ok,#state{verbose = Verbose}}

persist(Model, State) ->
    ModelName = ai_db_model:model_name(Model),
    IdField = ai_db_schema:id_name(ModelName),
    Id = ai_db_model:get_field(IdField, Model),
    NewId = case Id of
                undefined -> new_id(ModelName, sumo_internal:id_field_type(ModelName));
                Id        -> Id
            end,

  Model2 = sleep(Model),
  Fields = sumo_internal:doc_fields(Model2),
  Schema = sumo_internal:get_schema(ModelName),
  [IdField | NPFields] = schema_field_names(Schema),
  NPValues = [maps:get(K, Fields, undefined) || K <- NPFields],
  MnesiaRecord = list_to_tuple([ModelName, NewId | NPValues]),

  case mnesia:transaction(fun() -> mnesia:write(MnesiaRecord) end) of
    {aborted, Reason} ->
      {error, Reason, State};
    {atomic, ok} ->
      NewModel = sumo_internal:set_field(IdField, NewId, Model),
      _ = maybe_log(persist, [ModelName, NewModel], State),
      {ok, NewModel, State}
  end.

-spec fetch(ModelName, Id, State) -> Response when
  ModelName  :: sumo:schema_name(),
  Id       :: sumo:field_value(),
  State    :: state(),
  Response :: sumo_store:result(sumo_internal:doc(), state()).
fetch(ModelName, Id, State) ->
  try
    [Result] = mnesia:dirty_read(ModelName, Id),
    Schema = sumo_internal:get_schema(ModelName),
    Fields = schema_field_names(Schema),
    _ = maybe_log(fetch, [ModelName, Id], State),
    {ok, wakeup(result_to_doc(Result, Fields)), State}
  catch
    _:_ -> {error, notfound, State}
  end.

-spec delete_by(ModelName, Conditions, State) -> Response when
  ModelName    :: sumo:schema_name(),
  Conditions :: sumo:conditions(),
  State      :: state(),
  Response   :: sumo_store:result(sumo_store:affected_rows(), state()).
delete_by(ModelName, Conditions, State) ->
  MatchSpec = build_match_spec(ModelName, Conditions),
  Transaction = fun() ->
    Items = mnesia:select(ModelName, MatchSpec),
    lists:foreach(fun mnesia:delete_object/1, Items),
    length(Items)
  end,
  case mnesia:transaction(Transaction) of
    {aborted, Reason} ->
      {error, Reason, State};
    {atomic, Result} ->
      _ = maybe_log(delete_by, [ModelName, Conditions], State),
      {ok, Result, State}
  end.

-spec delete_all(ModelName, State) -> Response when
  ModelName  :: sumo:schema_name(),
  State    :: state(),
  Response :: sumo_store:result(sumo_store:affected_rows(), state()).
delete_all(ModelName, State) ->
  Count = mnesia:table_info(ModelName, size),
  case mnesia:clear_table(ModelName) of
    {atomic, ok} ->
      _ = maybe_log(delete_all, [ModelName], State),
      {ok, Count, State};
    {aborted, Reason} ->
      {error, Reason, State}
  end.

-spec find_all(ModelName, State) -> Response when
  ModelName  :: sumo:schema_name(),
  State    :: state(),
  Response :: sumo_store:result([sumo_internal:doc()], state()).
find_all(ModelName, State) ->
  find_all(ModelName, [], 0, 0, State).

-spec find_all(ModelName, SortFields, Limit, Offset, State) -> Response when
  ModelName    :: sumo:schema_name(),
  SortFields :: term(),
  Limit      :: non_neg_integer(),
  Offset     :: non_neg_integer(),
  State      :: state(),
  Response   :: sumo_store:result([sumo_internal:doc()], state()).
find_all(ModelName, SortFields, Limit, Offset, State) ->
  find_by(ModelName, [], SortFields, Limit, Offset, State).

-spec find_by(ModelName, Conditions, State) -> Response when
  ModelName    :: sumo:schema_name(),
  Conditions :: sumo:conditions(),
  State      :: state(),
  Response   :: sumo_store:result([sumo_internal:doc()], state()).
find_by(ModelName, Conditions, State) ->
  find_by(ModelName, Conditions, [], 0, 0, State).

-spec find_by(ModelName, Conditions, Limit, Offset, State) -> Response when
  ModelName    :: sumo:schema_name(),
  Conditions :: sumo:conditions(),
  Limit      :: non_neg_integer(),
  Offset     :: non_neg_integer(),
  State      :: state(),
  Response   :: sumo_store:result([sumo_internal:doc()], state()).
find_by(ModelName, Conditions, Limit, Offset, State) ->
  find_by(ModelName, Conditions, [], Limit, Offset, State).

-spec find_by(ModelName, Conditions, Sort, Limit, Offset, State) -> Response when
  ModelName    :: sumo:schema_name(),
  Conditions :: sumo:conditions(),
  Sort       :: term(),
  Limit      :: non_neg_integer(),
  Offset     :: non_neg_integer(),
  State      :: state(),
  Response   :: sumo_store:result([sumo_internal:doc()], state()).
find_by(ModelName, Conditions, [], Limit, Offset, State) ->
  MatchSpec = build_match_spec(ModelName, Conditions),
  Transaction0 = fun() ->
    mnesia:select(ModelName, MatchSpec)
  end,
  TransactionL = fun() ->
    case mnesia:select(ModelName, MatchSpec, Offset + Limit, read) of
      {ManyItems, _Cont} when length(ManyItems) >= Offset ->
        lists:sublist(ManyItems, Offset + 1, Limit);
      {_ManyItems, _Cont} ->
        [];
      '$end_of_table' ->
        []
    end
  end,
  Transaction = case Limit of
    0     -> Transaction0;
    Limit -> TransactionL
  end,
  case mnesia:transaction(Transaction) of
    {aborted, Reason} ->
      {error, Reason, State};
    {atomic, Results} ->
      Schema = sumo_internal:get_schema(ModelName),
      Fields = schema_field_names(Schema),
      Models = [wakeup(result_to_doc(Result, Fields)) || Result <- Results],
      _ = maybe_log(find_by, [ModelName, Conditions, Limit, Offset, MatchSpec], State),
      {ok, Models, State}
  end;
find_by(_ModelName, _Conditions, _Sort, _Limit, _Offset, State) ->
  {error, not_supported, State}.

-spec create_schema(Schema, State) -> Response when
  Schema   :: sumo:schema(),
  State    :: state(),
  Response :: sumo_store:result(state()).
create_schema(Schema, #{default_options := DefaultOptions} = State) ->
  Name = sumo_internal:schema_name(Schema),
  Fields = schema_fields(Schema),
  Attributes = [sumo_internal:field_name(Field) || Field <- Fields],
  Indexes = [
    sumo_internal:field_name(Field)
    || Field <- Fields, lists:member(index, sumo_internal:field_attrs(Field))
  ],
  Options = [
    {attributes, Attributes},
    {index, Indexes}
    | DefaultOptions
  ],
  case mnesia:create_table(Name, Options) of
    {atomic, ok}                      -> {ok, State};
    {aborted, {already_exists, Name}} -> {ok, State};
    {aborted, Reason}                 -> {error, Reason, State}
  end.

-spec count(ModelName, State) -> Response when
  ModelName  :: sumo:schema_name(),
  State    :: state(),
  Response :: sumo_store:result(non_neg_integer(), state()).
count(ModelName, State) ->
  try
    Size = mnesia:table_info(ModelName, size),
    {ok, Size, State}
  catch
    _:Reason -> {error, Reason, State}
  end.

-spec count_by(ModelName, Conditions, State) -> Response when
  ModelName    :: sumo:schema_name(),
  Conditions :: sumo:conditions(),
  State      :: state(),
  Response   :: sumo_store:result(non_neg_integer(), state()).
count_by(ModelName, Conditions, State) ->
  MatchSpec = build_match_spec(ModelName, Conditions),
  Transaction = fun() ->
    length(mnesia:select(ModelName, MatchSpec))
  end,
  case mnesia:transaction(Transaction) of
    {aborted, Reason} -> {error, Reason, State};
    {atomic, Count}   -> {ok, Count, State}
  end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%% @private
parse(Options) -> parse(Options, []).

%% @private
parse([], Acc) ->
  Acc;
parse([{disc_copies, here} | Options], Acc) ->
  parse(Options, [{disc_copies, [node()]} | Acc]);
parse([{disc_copies, Nodes} | Options], Acc) ->
  parse(Options, [{disc_copies, Nodes} | Acc]);
parse([{disc_only_copies, here} | Options], Acc) ->
  parse(Options, [{disc_only_copies, [node()]} | Acc]);
parse([{disc_only_copies, Nodes} | Options], Acc) ->
  parse(Options, [{disc_only_copies, Nodes} | Acc]);
parse([{ram_copies, here} | Options], Acc) ->
  parse(Options, [{ram_copies, [node()]} | Acc]);
parse([{ram_copies, Nodes} | Options], Acc) ->
  parse(Options, [{ram_copies, Nodes} | Acc]);
parse([{majority, Flag} | Options], Acc) ->
  parse(Options, [{majority, Flag} | Acc]);
parse([{snmp, SnmpStruct} | Options], Acc) ->
  parse(Options, [{snmp, SnmpStruct} | Acc]);
parse([{storage_properties, Props} | Options], Acc) ->
  parse(Options, [{storage_properties, Props} | Acc]);
parse([_IgnoredOption | Options], Acc) ->
  parse(Options, Acc).

new_id(ModelName, FieldType) ->
  NewId = new_id(FieldType),
  case mnesia:dirty_read(ModelName, NewId) of
    [] -> NewId;
    _  -> new_id(ModelName, FieldType)
  end.

new_id(string)    -> ai_uuid:uuid_to_string(ai_uuid:get_v4(), standard);
new_id(binary)    -> ai_uuid:uuid_to_string(ai_uuid:get_v4(), binary_standard);
new_id(integer)   -> <<Id:128>> = ai_uuid:get_v4(), Id;
new_id(float)     -> <<Id:128>> = ai_uuid:get_v4(), Id * 1.0;
new_id(FieldType) -> exit({unimplemented, FieldType}).

%% @doc http://www.erlang.org/doc/apps/erts/match_spec.html
%% @private
build_match_spec(ModelName, Condition) when not is_list(Condition) ->
  build_match_spec(ModelName, [Condition]);
build_match_spec(ModelName, Conditions) ->
  NewConditions = transform_conditions(ModelName, Conditions),
  Schema = sumo_internal:get_schema(ModelName),
  Fields = schema_field_names(Schema),
  FieldsMap = maps:from_list(
    [field_tuple(I, Fields) || I <- lists:seq(1, length(Fields))]),
  % The following ordering function avoids '$10' been added between
  % '$1' and '$2' in the MatchHead list. Without this fix, this store
  % would fail when trying to use `find_by` function.
  OrderingFun = fun(A, B) ->
    "$" ++ ANumber = atom_to_list(A),
    "$" ++ BNumber = atom_to_list(B),
    list_to_integer(ANumber) =< list_to_integer(BNumber)
  end,
  ValuesSorted = lists:sort(OrderingFun, maps:values(FieldsMap)),
  MatchHead = list_to_tuple([ModelName | ValuesSorted]),
  Guard = [
    condition_to_guard(Condition, FieldsMap) || Condition <- NewConditions
  ],
  Result = '$_',
  [{MatchHead, Guard, [Result]}].

%% @private
field_tuple(I, Fields) ->
  FieldName = lists:nth(I, Fields),
  FieldWildcard = list_to_atom([$$ | integer_to_list(I)]),
  {FieldName, FieldWildcard}.

%% @private
condition_to_guard({'and', [Expr1]}, FieldsMap) ->
  condition_to_guard(Expr1, FieldsMap);
condition_to_guard({'and', [Expr1 | Exprs]}, FieldsMap) ->
  { 'andalso'
  , condition_to_guard(Expr1, FieldsMap)
  , condition_to_guard({'and', Exprs}, FieldsMap)
  };
condition_to_guard({'or', [Expr1]}, FieldsMap) ->
  condition_to_guard(Expr1, FieldsMap);
condition_to_guard({'or', [Expr1 | Exprs]}, FieldsMap) ->
  { 'orelse'
  , condition_to_guard(Expr1, FieldsMap)
  , condition_to_guard({'or', Exprs}, FieldsMap)
  };
condition_to_guard({'not', Expr}, FieldsMap) ->
  {'not', condition_to_guard(Expr, FieldsMap)};
condition_to_guard({Name1, Op, Name2}, FieldsMap) when is_atom(Name2) ->
  check_operator(Op),
  %NOTE: Name2 can be a field name or a value, that's why the following happens
  {Op, maps:get(Name1, FieldsMap), maps:get(Name2, FieldsMap, {const, Name2})};
condition_to_guard({Name1, Op, Value}, FieldsMap) ->
  check_operator(Op),
  {Op, maps:get(Name1, FieldsMap), {const, Value}};
condition_to_guard({Name, 'null'}, FieldsMap) ->
  condition_to_guard({Name, '==', undefined}, FieldsMap);
condition_to_guard({Name, 'not_null'}, FieldsMap) ->
  condition_to_guard({Name, '/=', undefined}, FieldsMap);
condition_to_guard({Name, Value}, FieldsMap) ->
  condition_to_guard({Name, '==', Value}, FieldsMap).

%% @private
check_operator(like) -> exit({unsupported_operator, like});
check_operator(Op)   -> sumo_internal:check_operator(Op).

%% @private
schema_field_names(Schema) ->
  [sumo_internal:field_name(Field) || Field <- schema_fields(Schema)].

%% @private
schema_fields(Schema) ->
  place_id_first(sumo_internal:schema_fields(Schema)).

%% @private
place_id_first(Fields) ->
  place_id_first(Fields, []).
place_id_first([], Acc) ->
  lists:reverse(Acc);
place_id_first([Field|Fields], Acc) ->
  case lists:member(id, sumo_internal:field_attrs(Field)) of
    true  -> [Field|lists:reverse(Acc)] ++ Fields;
    false -> place_id_first(Fields, [Field|Acc])
  end.

%% @private
result_to_doc(Result, Fields) ->
  [ModelName | Values] = tuple_to_list(Result),
  NewModel = sumo_internal:new_doc(ModelName),
  Pairs = lists:zip(Fields, Values),
  lists:foldl(fun({Name, Value}, Model) ->
    sumo_internal:set_field(Name, Value, Model)
  end, NewModel, Pairs).

%% @private
transform_conditions(ModelName, Conditions) ->
  sumo_utils:transform_conditions(fun validate_date/1, ModelName, Conditions, [date]).

%% @private
validate_date({FieldType, _, FieldValue}) ->
  case {FieldType, sumo_utils:is_datetime(FieldValue)} of
    {date, true} ->
      {FieldValue, {0, 0, 0}}
  end.

%% @private
sleep(Model) ->
  sumo_utils:doc_transform(fun sleep_fun/4, Model).

%% @private
sleep_fun(_, _, undefined, _) ->
  undefined;
sleep_fun(string, _, FieldValue, _) ->
  sumo_utils:to_bin(FieldValue);
sleep_fun(date, _, FieldValue, _) ->
  case sumo_utils:is_datetime(FieldValue) of
    true -> {FieldValue, {0, 0, 0}};
    _    -> FieldValue
  end;
sleep_fun(_, _, FieldValue, _) ->
  FieldValue.

%% @private
wakeup(Model) ->
  sumo_utils:doc_transform(fun wakeup_fun/4, Model).

%% @private
wakeup_fun(date, _, {Date, _} = _FieldValue, _) ->
  Date;
wakeup_fun(_, _, FieldValue, _) ->
  FieldValue.

%% @private
maybe_log(Fun, Args, #{verbose := true}) ->
  error_logger:info_msg(log_format(Fun), Args);
maybe_log(_, _, _) ->
  ok.

%% @private
log_format(persist)    -> "persist(~p, ~p)";
log_format(fetch)      -> "fetch(~p, ~p)";
log_format(delete_by)  -> "delete_by(~p, ~p)";
log_format(delete_all) -> "delete_all(~p)";
log_format(find_by)    -> "find_by(~p, ~p, [], ~p, ~p)~nMatchSpec: ~p".
