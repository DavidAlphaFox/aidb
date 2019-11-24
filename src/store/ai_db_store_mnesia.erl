-module(ai_db_store_mnesia).

%% API
-export([count/2, count_by/3, delete_all/2, delete_by/3,
	 dirty/2, fetch/3, find_all/2, find_all/5, find_by/3,
	 find_by/5, find_by/6, init/1, persist/2,
	 transaction/2]).

-record(state, {verbose}).

%%%=============================================================================
%%% API
%%%=============================================================================
init(Options) ->
    Verbose = maps:get(verbose, Options, false),
    {ok, #state{verbose = Verbose}}.

persist(Model, State) ->
    ModelName = ai_db_model:model_name(Model),
    IdField = ai_db_schema:id_name(ModelName),
    Id = ai_db_model:get_field(IdField, Model),
    NewId = case Id of
	      undefined ->
		  new_id(ModelName, ai_db_schema:id_type(ModelName));
	      Id -> Id
	    end,
    Model2 = ai_db_transform:model(fun sleep/4, Model),
    Fields = ai_db_model:model_fields(Model2),
    Schema = ai_db_schema:schema(ModelName),
    [IdField | NPFields] = schema_field_names(Schema),
    NPValues = [maps:get(K, Fields, undefined)
		|| K <- NPFields],
    MnesiaRecord = list_to_tuple([ModelName, NewId
				  | NPValues]),
    case mnesia:transaction(fun () ->
				    mnesia:write(MnesiaRecord)
			    end)
	of
      {aborted, Reason} -> {{error, Reason}, State};
      {atomic, ok} ->
	  NewModel = ai_db_model:set_field(IdField, NewId, Model),
	  _ = maybe_log(persist, [ModelName, NewModel], State),
	  {{ok, NewModel}, State}
    end.

fetch(ModelName, ID, State) ->
    try [Result] = mnesia:dirty_read(ModelName, ID),
	Schema = ai_db_schema:schema(ModelName),
	Fields = schema_field_names(Schema),
	_ = maybe_log(fetch, [ModelName, ID], State),
	{{ok,
	  ai_db_transform:model(fun wakeup/4,
				result_to_model(Result, Fields))},
	 State}
    catch
      _:_ -> {{error, not_found}, State}
    end.

delete_by(ModelName, Conditions, State) ->
    MatchSpec = build_match_spec(ModelName, Conditions),
    Transaction = fun () ->
			  Items = mnesia:select(ModelName, MatchSpec),
			  lists:foreach(fun mnesia:delete_object/1, Items),
			  erlang:length(Items)
		  end,
    case mnesia:transaction(Transaction) of
      {aborted, Reason} -> {{error, Reason}, State};
      {atomic, Result} ->
	  _ = maybe_log(delete_by, [ModelName, MatchSpec], State),
	  {{ok, Result}, State}
    end.

delete_all(ModelName, State) ->
    Count = mnesia:table_info(ModelName, size),
    case mnesia:clear_table(ModelName) of
      {atomic, ok} ->
	  _ = maybe_log(delete_all, [ModelName], State),
	  {{ok, Count}, State};
      {aborted, Reason} -> {{error, Reason}, State}
    end.

find_all(ModelName, State) ->
    find_all(ModelName, [], 0, 0, State).

find_all(ModelName, SortFields, Limit, Offset, State) ->
    find_by(ModelName, [], SortFields, Limit, Offset, State).

find_by(ModelName, Conditions, State) ->
    find_by(ModelName, Conditions, [], 0, 0, State).

find_by(ModelName, Conditions, Limit, Offset, State) ->
    find_by(ModelName, Conditions, [], Limit, Offset,State).

find_by(ModelName, Conditions, [], Limit, Offset,State) ->
  MatchSpec = build_match_spec(ModelName, Conditions),
  Transaction0 = fun () ->
		mnesia:select(ModelName, MatchSpec)
	end,
  TransactionL = fun () ->
    case mnesia:select(ModelName, MatchSpec, Offset + Limit, read) of
			{ManyItems, _Cont} when length(ManyItems) >= Offset ->
				lists:sublist(ManyItems, Offset + 1, Limit);
			{_ManyItems, _Cont} -> [];
			  '$end_of_table' -> []
		end
	end,
  Transaction = 
    case Limit of
		  0 -> Transaction0;
		  Limit -> TransactionL
		end,
  case mnesia:transaction(Transaction) of
    {aborted, Reason} -> {{error, Reason}, State};
    {atomic, Results} ->
	    Schema = ai_db_schema:schema(ModelName),
	    Fields = schema_field_names(Schema),
	    Models = [ai_db_transform:model(fun wakeup/4,result_to_model(Result, Fields))
		    || Result <- Results],
	    _ = maybe_log(find_by,
			  [ModelName, Conditions, Limit, Offset, MatchSpec],
		  	State),
	    {{ok, Models}, State}
  end;
find_by(_ModelName, _Conditions, _Sort, _Limit, _Offset,State) ->
  {{error, not_supported}, State}.

count(ModelName, State) ->
  try 
    Size = mnesia:table_info(ModelName, size),
	  {{ok, Size}, State}
  catch
    _:Reason -> {{error, Reason}, State}
  end.

count_by(ModelName, Conditions, State) ->
    MatchSpec = build_match_spec(ModelName, Conditions),
    Transaction = fun () ->
			  erlang:length(mnesia:select(ModelName, MatchSpec))
		  end,
    case mnesia:transaction(Transaction) of
      {aborted, Reason} -> {{error, Reason}, State};
      {atomic, Count} -> {{ok, Count}, State}
    end.

dirty(Fun, State) -> transaction(Fun, State).

transaction(Fun, State) ->
    case mnesia:transaction(Fun) of
      {aborted, Reason} -> {{error, Reason}, State};
      {atomic, Result} -> {{ok, Result}, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_id(ModelName, FieldType) ->
    NewId = new_id(FieldType),
    case mnesia:dirty_read(ModelName, NewId) of
      [] -> NewId;
      _ -> new_id(ModelName, FieldType)
    end.

new_id(string) ->
    ai_uuid:uuid_to_string(ai_uuid:get_v4(), standard);
new_id(binary) ->
    ai_uuid:uuid_to_string(ai_uuid:get_v4(),
			   binary_standard);
new_id(integer) -> <<Id:128>> = ai_uuid:get_v4(), Id;
new_id(float) ->
    <<Id:128>> = ai_uuid:get_v4(), Id * 1.0;
new_id(FieldType) -> exit({unimplemented, FieldType}).

sleep(_, _, undefined, _) -> undefined;
sleep(string, _, FieldValue, _) ->
    ai_string:to_string(FieldValue);
sleep(date, _, FieldValue, _) ->
    case ai_db_type:is_datetime(FieldValue) of
      true -> {FieldValue, {0, 0, 0}};
      _ -> FieldValue
    end;
sleep(_, _, FieldValue, _) -> FieldValue.

wakeup(date, _, {Date, _} = _FieldValue, _) -> Date;
wakeup(_, _, FieldValue, _) -> FieldValue.

result_to_model(Result, Fields) ->
    [ModelName | Values] = erlang:tuple_to_list(Result),
    Pairs = maps:from_list(lists:zip(Fields, Values)),
    ai_db_model:new_model(ModelName, Pairs).

transform_conditions(ModelName, Conditions) ->
    ai_db_transform:conditions(fun validate_date/1,
			       ModelName, Conditions, [date]).

validate_date({FieldType, _, FieldValue}) ->
    case {FieldType, ai_db_type:is_datetime(FieldValue)} of
      {date, true} -> {FieldValue, {0, 0, 0}}
    end.

%% @doc http://www.erlang.org/doc/apps/erts/match_spec.html
build_match_spec(ModelName, Condition)
    when not erlang:is_list(Condition) ->
    build_match_spec(ModelName, [Condition]);
build_match_spec(ModelName, Conditions) ->
    NewConditions = transform_conditions(ModelName,
					 Conditions),
    Schema = ai_db_schema:schema(ModelName),
    Fields = schema_field_names(Schema),
    FieldsMap = maps:from_list([field_tuple(I, Fields)
				|| I <- lists:seq(1, erlang:length(Fields))]),
    % The following ordering function avoids '$10' been added between
    % '$1' and '$2' in the MatchHead list. Without this fix, this store
    % would fail when trying to use `find_by` function.
    OrderingFun = fun (A, B) ->
			  "$" ++ ANumber = erlang:atom_to_list(A),
			  "$" ++ BNumber = erlang:atom_to_list(B),
			  erlang:list_to_integer(ANumber) =<
			    erlang:list_to_integer(BNumber)
		  end,
    ValuesSorted = lists:sort(OrderingFun,
			      maps:values(FieldsMap)),
    MatchHead = erlang:list_to_tuple([ModelName
				      | ValuesSorted]),
    Guard = [condition_to_guard(Condition, FieldsMap)
	     || Condition <- NewConditions],
    Result = '$_',
    [{MatchHead, Guard, [Result]}].

field_tuple(I, Fields) ->
    FieldName = lists:nth(I, Fields),
    FieldWildcard = erlang:list_to_atom([$$
					 | erlang:integer_to_list(I)]),
    {FieldName, FieldWildcard}.

schema_field_names(Schema) ->
    [ai_db_schema:field_name(Field)
     || Field <- schema_fields(Schema)].

schema_fields(Schema) ->
    place_id_first(ai_db_schema:schema_fields(Schema)).

place_id_first(Fields) -> place_id_first(Fields, []).

place_id_first([], Acc) -> lists:reverse(Acc);
place_id_first([Field | Fields], Acc) ->
    case lists:member(id, ai_db_schema:field_attrs(Field))
	of
      true -> [Field | lists:reverse(Acc)] ++ Fields;
      false -> place_id_first(Fields, [Field | Acc])
    end.

condition_to_guard({'and', [Expr1]}, FieldsMap) ->
    condition_to_guard(Expr1, FieldsMap);
condition_to_guard({'and', [Expr1 | Exprs]},
		   FieldsMap) ->
    {'andalso', condition_to_guard(Expr1, FieldsMap),
     condition_to_guard({'and', Exprs}, FieldsMap)};
condition_to_guard({'or', [Expr1]}, FieldsMap) ->
    condition_to_guard(Expr1, FieldsMap);
condition_to_guard({'or', [Expr1 | Exprs]},
		   FieldsMap) ->
    {'orelse', condition_to_guard(Expr1, FieldsMap),
     condition_to_guard({'or', Exprs}, FieldsMap)};
condition_to_guard({'not', Expr}, FieldsMap) ->
    {'not', condition_to_guard(Expr, FieldsMap)};
condition_to_guard({Name1, Op, Name2}, FieldsMap)
    when is_atom(Name2) ->
    check_operator(Op),
    {Op, maps:get(Name1, FieldsMap),
     maps:get(Name2, FieldsMap, {const, Name2})};
condition_to_guard({Name1, Op, Value}, FieldsMap) ->
    check_operator(Op),
    {Op, maps:get(Name1, FieldsMap), {const, Value}};
condition_to_guard({Name, null}, FieldsMap) ->
    condition_to_guard({Name, '==', undefined}, FieldsMap);
condition_to_guard({Name, not_null}, FieldsMap) ->
    condition_to_guard({Name, '/=', undefined}, FieldsMap);
condition_to_guard({Name, Value}, FieldsMap) ->
    condition_to_guard({Name, '==', Value}, FieldsMap).

check_operator(like) ->
    exit({unsupported_operator, like});
check_operator('<') -> ok;
check_operator('=<') -> ok;
check_operator('>') -> ok;
check_operator('>=') -> ok;
check_operator('==') -> ok;
check_operator('/=') -> ok.

maybe_log(Fun, Args, #state{verbose = true}) ->
    error_logger:info_msg(log_format(Fun), Args);
maybe_log(_, _, _) -> ok.

log_format(persist) -> "persist(~p, ~p)";
log_format(fetch) -> "fetch(~p, ~p)";
log_format(delete_by) -> "delete_by(~p, ~p)";
log_format(delete_all) -> "delete_all(~p)";
log_format(find_by) ->
    "find_by(~p, ~p, [], ~p, ~p)~nMatchSpec: ~p".
