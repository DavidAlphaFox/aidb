-module(ai_db_store_postgres).


-export([init/1]).

-export([
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
-include_lib("epgsql/include/epgsql.hrl").
-record(state, {conn, args}).

init(Args) -> {ok,#state{conn = undefined, args=Args}}.

persist(Model,State)->
    ModelName = ai_db_model:model_name(Model),
    IDField = ai_db_schema:id_name(ModelName),
    ID = ai_db_model:get_field(IDField,Model),

    TableName = escape_field(ModelName),

    Fields = ai_db_model:model_fields(Model),
    NPFields = maps:remove(IDField, Fields),
    NPFieldNames = maps:keys(NPFields),
    NPColumnNames = lists:map(fun escape_field/1, NPFieldNames),

    Schema = ai_db_schema:schema(ModelName),
    SchemaFields = ai_db_schema:schema_fields(Schema),
    ColumnTypes = [
                   {ai_db_schema:field_name(F), ai_db_schema:field_type(F),ai_db_schema:field_attrs(F)}
                   || F <- SchemaFields
                  ],
    NPColumnValues = lists:map(fun (N) ->
                                       {N, _T, _A} = lists:keyfind(N, 1, ColumnTypes),
                                       maps:get(N, Fields)
                               end, NPFieldNames),
    IDFieldBin = escape_field(IDField),
    {Sql, Values} =
        case ID of
            undefined ->
                NPColumnsNamesCSV = ai_string:join(NPColumnNames, <<",">>),
                SlotsFun = fun(N) -> NBin = erlang:integer_to_binary(N), <<"$",NBin/binary>>  end,
                InsertSlots = lists:map(SlotsFun, lists:seq(1, erlang:length(NPFieldNames))),
                InsertSlotsCSV = string:join(InsertSlots, <<", ">>),
                InsertSql = <<"INSERT INTO ", TableName/binary," ( ", NPColumnsNamesCSV/binary, " ) ",
                              " VALUES "," ( ", InsertSlotsCSV/binary, " ) ",
                              "RETURNING ", IDFieldBin/binary>>,
                {InsertSql, NPColumnValues};
            _ ->
                UpdateFun = fun(FieldName, {N, Slots}) ->
                                    NBin = erlang:integer_to_binary(N),
                                    Slot = <<FieldName/binary, " = $", NBin/binary>>,
                                    {N + 1, [Slot | Slots]}
                            end,
                {IDIndex, UpdateSlots} = lists:foldl(UpdateFun, {1, []}, NPColumnNames),
                UpdateSlotsCSV = string:join(UpdateSlots, <<",">>),
                IDIndexBin = erlang:integer_to_binary(IDIndex),
                IDSlot = <<"$",IDIndexBin/binary>>,
                UpdateSql = <<"UPDATE ", TableName/binary," SET ",UpdateSlotsCSV/binary,
                              " WHERE ", IDFieldBin/binary, " = ",IDSlot/binary>>,
                UpdateValues = NPColumnValues ++ [ID],
                {UpdateSql, UpdateValues}
        end,
    ToNullFun = fun
                    (undefined) -> null;
                    (Value) -> Value
                end,
    ProcessedValues = lists:map(ToNullFun, Values),
    Fun = fun( Conn ) ->  epgsql:equery(Conn, Sql, ProcessedValues) end,
    case transaction(Fun,State) of
        {{ok, _Count, _Columns, Rows},NewState} ->
            {LastId} = erlang:hd(Rows),
            NewModel = ai_db_model:set_field(IDField, LastId, Model),
            {{ok, NewModel}, NewState};
        {{ok, 1},NewState} -> {{ok, Model}, NewState};
        {{ok,_Count},NewState} -> {{error,not_persist},NewState};
        {Error, NewState} -> {Error, NewState}
  end.
fetch(ModelName,ID,State)->
    IdFieldName = ai_db_schema:id_name(ModelName),
  case find_by(ModelName, [{IdFieldName, ID}], [], 1, 0, State) of
    {{ok, [Model]},NewState} -> {{ok,Model}, NewState};
    {{ok, []}, NewState}    -> {{error, not_found}, NewState};
    Error          -> Error
  end.

delete_by(ModelName,Conditions,State) ->
    TableName = escape_field(ModelName),
    {Values, CleanConditions} = prepare_conditions(Conditions),
    Clauses = conditions_to_where(CleanConditions),
    Sql = <<"DELETE FROM ",TableName/binary," WHERE ", Clauses/binary>>,
    Fun = fun(Conn) -> epgsql:equery(Conn, Sql, Values) end,
    transaction(Fun,State).

delete_all(ModelName,State) ->
    TableName = escape_field(ModelName),
    Sql = <<"DELETE FROM ",TableName/binary>>,
    Fun = fun(Conn) -> epgsql:equery(Conn, Sql,[]) end,
    transaction(Fun,State).
find_by(ModelName,Conditions,State) ->
    find_by(ModelName,Conditions,[],0,0,State).
find_by(ModelName,Conditions,Limit,Offset,State) ->
    find_by(ModelName,Conditions,[],Limit,Offset,State).
find_by(ModelName,Conditions,Sort,Limit,Offset,State) ->
    {Values, CleanConditions} = prepare_conditions(Conditions),
    Clauses = conditions_to_where(CleanConditions),
    TableName = escape_field(ModelName),
    OrderByClause =
        case Sort of
            [] -> <<>>;
            undefined -> <<>>;
            _  -> sort_to_order_by(Sort)
        end,
    WhereClause =
        case erlang:byte_size(Clauses) of
            0 -> <<>>;
            _ -> <<" WHERE ", Clauses/binary>>
        end,
    Sql1 = <<"SELECT * FROM ", TableName/binary,
             WhereClause/binary,OrderByClause/binary>>,
    Sql2 =
        case Limit of
            0 -> Sql1;
            _ ->
                Count = erlang:length(Values),
                LimitSlot = erlang:integer_to_binary(Count + 1),
                OffsetSlot = erlang:integer_to_binary(Count + 2),
                <<Sql1/binary," LIMIT $",LimitSlot/binary," OFFSET $",OffsetSlot/binary>>
        end,
    AllValues =
        case Limit of
            0     -> Values;
            Limit -> Values ++ [Limit, Offset]
        end,
    Fun = fun( Conn ) ->  epgsql:equery(Conn, Sql2, AllValues) end,
    case transaction(Fun,State) of
        {{ok, Columns, Rows},NewState} ->
            ColFun = fun(Col) -> erlang:binary_to_atom(Col#column.name, utf8) end,
            ColumnNames = lists:map(ColFun, Columns),
            FoldFun = fun({Name, Value}, Model) ->
                              ai_db_model:set_field(Name, Value,Model)
                      end,
            RowFun = fun(Row) ->
                             Fields = erlang:tuple_to_list(Row),
                             Pairs = lists:zip(ColumnNames, Fields),
                             Model = ai_db_model:new_model(ModelName),
                             lists:foldl(FoldFun, Model, Pairs)
                     end,
            Models = lists:map(RowFun, Rows),
            {{ok, Models}, NewState};
        Error -> Error
  end.



find_all(ModelName,State) ->  find_all(ModelName, [], 0, 0, State).
find_all(ModelName,Sort,Limit,Offset,State) ->   find_by(ModelName, [], Sort, Limit, Offset, State).
count(ModelName,State) ->
    TableName = escape_field(ModelName),
    Query = <<"SELECT Count(*) FROM ", TableName/binary>>,
    Fun = fun(Conn) -> epgsql:squery(Conn,Query) end,
    case dirty(Fun,State) of
    {{ok, _, [{Count}]},NewState} ->
      {{ok, erlang:binary_to_integer(Count)}, NewState};
    Error -> Error
  end.
count_by(ModelName, undefined, State) ->
  count(ModelName, State);
count_by(ModelName, [], State) ->
  count(ModelName, State);
count_by(ModelName,Conditions,State) ->
    {Values, CleanConditions} = prepare_conditions(Conditions),
    Clauses = conditions_to_where(CleanConditions),
    TableName = escape_field(ModelName),
    Sql = <<"SELECT COUNT(1) FROM ",TableName/binary," WHERE ",Clauses/binary>>,
    Fun = fun(Conn) -> epgsql:equery(Conn,Sql, Values) end,
    case dirty(Fun,State) of
        {{ok, _, [{Count}]},NewState} -> {{ok, Count}, NewState};
        Error -> Error
    end.



dirty(Fun,State)->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R =  Fun(Conn),
                {R,NewState}
            catch
                _Type:Reason->
                    catch epgsql:close(Conn),
                    {{error,Reason},NewState#state{conn = undefined}}
            end;
        Error -> Error
    end.

transaction(Fun,State) ->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R = epgsql:with_transaction(Conn,Fun,[{ensure_committed,false},{reraise,true}]),
                {R,NewState}
            catch
                _Type:Reason->
                    catch epgsql:close(Conn),
                    {{error,Reason},NewState#state{conn = undefined}}
            end;
        Error-> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

alive(Conn) ->
    try
        {ok,_} = epgsql:squery(Conn,"SELECT 1 "),
        true
    catch
        _:_-> false
    end.

connect(#state{conn = Conn} = State) when is_pid(Conn) ->
    case is_process_alive(Conn) and alive(Conn) of
        true  -> {ok, Conn,State};
        false -> connect(State#state{conn = undefined})
    end;

connect(#state{conn = undefined, args = Args} = State) ->
    case epgsql:connect(Args) of
        {ok, Conn} -> {ok, Conn,State#state{conn = Conn}};
        Error -> {Error,State}
    end.


prepare_conditions(Expr)->
    {Values, CleanExprs, _} = prepare_conditions(Expr, {[], [], 1}),
    {lists:reverse(Values), lists:reverse(CleanExprs)}.

prepare_conditions(Exprs, Acc) when is_list(Exprs) ->
    lists:foldl(fun prepare_conditions/2, Acc, Exprs);

prepare_conditions({LogicalOp, Exprs}, {Values, CleanExprs, Count})
  when (LogicalOp == 'and')
       or (LogicalOp == 'or')
       or (LogicalOp == 'not') ->
    {NewValues, NewCleanExprs, NewCount} = prepare_conditions(Exprs, {Values, [], Count}),
    {NewValues,
        [{LogicalOp, lists:reverse(NewCleanExprs)} | CleanExprs],
    NewCount};
prepare_conditions({ Op,Name, Value}, {Values, CleanExprs, Count})
  when (not is_atom(Value)) andalso (not is_tuple(Value))->
    {[Value | Values],
     [{Op,Name, {'$', Count}} | CleanExprs],
    Count + 1};
prepare_conditions({Op,Name1, Name2}, {Values, CleanExprs, Count})
  when is_atom(Name2) orelse is_tuple(Name2) ->
    {Values,
     [{Op,Name1, Name2} | CleanExprs],
    Count};
prepare_conditions({Name, Value}, {Values, CleanExprs, Count})
  when Value =/= 'null' andalso Value =/= 'not_null' ->
    {[Value | Values],
     [{Name, {'$', Count}} | CleanExprs],
     Count + 1};
prepare_conditions({Name, Value}, {Values, CleanExprs, Count}) ->
    {Values,
        [{Name, Value} | CleanExprs],
    Count};
prepare_conditions([], Acc) -> Acc;
prepare_conditions(Expr, _) -> throw({unsupported,expression, Expr}).

conditions_to_where([]) -> <<>>;
conditions_to_where(Exprs) when erlang:is_list(Exprs) ->
    Clauses = [conditions_to_where(Expr) || Expr <- Exprs],
    ai_string:join(Clauses,<<" AND ">>);
conditions_to_where({'and', Exprs}) ->
    BinaryClauses = conditions_to_where(Exprs),
    <<" ( ",BinaryClauses/binary," ) ">>;
conditions_to_where({'or', Exprs}) ->
    Clauses = [conditions_to_where(Expr) || Expr <- Exprs],
    BinaryClauses = ai_string:join(Clauses,<<" OR ">>),
    <<" ( ",BinaryClauses/binary," ) ">>;
conditions_to_where({'not', Expr}) ->
    BinaryClauses = conditions_to_where(Expr),
    <<" NOT ( ",BinaryClauses/binary," ) ">>;
conditions_to_where({Op, Name, { '$', _ } = Slot}) ->
    P = placeholder(Slot),
    N = escape_field(Name),
    O = operator(Op),
    <<N/binary,O/binary,P/binary>>;
conditions_to_where({Op,Name1, {field,Name2}}) ->
    N1 = escape_field(Name1),
    N2 = escape_field(Name2),
    O = operator(Op),
    <<N1/binary,O/binary,N2>>;
conditions_to_where({Op,Name1,Name2}) ->
    N1 = escape_field(Name1),
    N2 = escape_field(Name2),
    O = operator(Op),
    <<N1/binary,O/binary,N2/binary>>;
conditions_to_where({Name, null}) ->
    N = escape_field(Name),
    <<N/binary," IS NULL ">>;
conditions_to_where({Name, not_null})->
    N = escape_field(Name),
    <<N/binary," IS NOT NULL ">>;
conditions_to_where({Name,Value}) ->
    N = escape_field(Name),
    V = escape_value(Value),
    <<N/binary," = ",V/binary>>.

sort_to_order_by(SortFields) ->
    ClauseFun = fun({Name, SortOrder}) ->
                        NameBin = ai_string:to_string(Name),
                        OrderBin = ai_string:to_string(SortOrder),
                        <<NameBin/binary," ",OrderBin/binary>>
                end,
    Clauses = lists:map(ClauseFun, SortFields),
    ClauseBin = ai_string:join(Clauses,<<",">>),
    <<" ORDER BY ", ClauseBin/binary>>.


operator('=<') -> <<"<=">>;
operator('/=') -> <<"!=">>;
operator('==') -> <<"=">>;
operator(Op) -> ai_string:to_string(Op).

placeholder({Prefix, N}) ->
    P = ai_string:to_string(Prefix),
    H = ai_string:to_string(N),
    <<$\s,P/binary,H/binary,$\s>>.

escape_field({'raw_as',Field,ASField})->
    AF = escape_field(ASField),
    <<Field/binary," AS ",AF/binary>>;
escape_field({'as',Field,ASField})->
    F = escape_field(Field),
    AF = escape_field(ASField),
    <<F/binary," AS ",AF/binary>>;
escape_field(Field) ->
    F = ai_string:to_string(Field),
    if
        F == <<"*">> -> F;
        true->
            F1 = re:replace(F,"\"","\\\"",[global,{return,binary}]),
            <<"\"",F1/binary,"\"">>
    end.

escape_value(Value)->
    F = ai_string:to_string(Value),
    case binary:match(F,<<"'">>) of
        nomatch ->  <<"",F,"'">>;
        _->
            F1 = re:replace(F,"'","\\'",[global,{return,binary}]),
            <<"E'",F1,"'">>
    end.
