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
    ModelName = ai_db_model:name(Model),
    Schema = ai_db_schema:schema(ModelName),
    IDField = ai_db_schema:id(Schema,name),
    ID = ai_db_model:get_field(IDField,Model),

    TableName = ai_postgres_utils:escape_field(ModelName),

    Fields = ai_db_model:fields(Model),
    NPFields = maps:remove(IDField, Fields),
    NPFieldNames = maps:keys(NPFields),
    NPColumnNames = lists:map(fun ai_postgres_utils:escape_field/1, NPFieldNames),

    SchemaFields = ai_db_schema:fields(Schema),
    ColumnTypes = [
                   {ai_db_field:name(F), ai_db_field:type(F),ai_db_field:attrs(F)}
                   || F <- SchemaFields
                  ],
    NPColumnValues = lists:map(
                       fun (N) ->
                               {N, _T, _A} = lists:keyfind(N, 1, ColumnTypes),
                               maps:get(N, Fields)
                       end, NPFieldNames),
    IDFieldBin = ai_postgres_utils:escape_field(IDField),
    {Sql, Values} =
        case ID of
            undefined ->
                NPColumnsNamesCSV = ai_string:join(NPColumnNames, <<",">>),
                SlotsFun = fun(N) -> NBin = erlang:integer_to_binary(N), <<"$",NBin/binary>>  end,
                InsertSlots = lists:map(SlotsFun, lists:seq(1, erlang:length(NPFieldNames))),
                InsertSlotsCSV = ai_string:join(InsertSlots, <<", ">>),
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
                UpdateSlotsCSV = ai_string:join(UpdateSlots, <<",">>),
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
    Schema = ai_db_schema:schema(ModelName),
    IDFieldName = ai_db_schema:id(Schema,name),
    case find_by(ModelName, [{IDFieldName, ID}], [], 1, 0, State) of
        {{ok, [Model]},NewState} -> {{ok,Model}, NewState};
        {{ok, []}, NewState}    -> {{error, not_found}, NewState};
        Error          -> Error
    end.

delete_by(ModelName,Conditions,State) ->
    TableName = ai_postgres_utils:escape_field(ModelName),
    {Values, CleanConditions} = ai_postgres_clause:prepare_conditions(Conditions),
    Clauses = ai_postgres_clause:where_clause(CleanConditions),
    Sql = <<"DELETE FROM ",TableName/binary," WHERE ", Clauses/binary>>,
    Fun = fun(Conn) -> epgsql:equery(Conn, Sql, Values) end,
    transaction(Fun,State).

delete_all(ModelName,State) ->
    TableName = ai_postgres_utils:escape_field(ModelName),
    Sql = <<"DELETE FROM ",TableName/binary>>,
    Fun = fun(Conn) -> epgsql:equery(Conn, Sql,[]) end,
    transaction(Fun,State).
find_by(ModelName,Conditions,State) ->
    find_by(ModelName,Conditions,[],0,0,State).
find_by(ModelName,Conditions,Limit,Offset,State) ->
    find_by(ModelName,Conditions,[],Limit,Offset,State).
find_by(ModelName,Conditions,Sort,Limit,Offset,State) ->
    {Values, CleanConditions} = ai_postgres_clause:prepare_conditions(Conditions),
    Clauses = ai_postgres_clause:where_clause(CleanConditions),
    TableName = ai_postgres_utils:escape_field(ModelName),
    OrderByClause =
        case Sort of
            undefined -> <<>>;
            [] -> <<>>;
            Sort when erlang:is_binary(Sort)-> <<" ORDER BY ",Sort/binary>>;
            Sort when erlang:is_list(Sort)  ->
                OrderByBin = ai_postgres_clause:order_by_clause(Sort),
                case erlang:byte_size(OrderByBin) of
                    0 -> <<>>;
                    _ -> <<" ORDER BY ",OrderByBin/binary>>
                end
        end,
    WhereClause =
        case erlang:byte_size(Clauses) of
            0 -> <<>>;
            _ -> <<" WHERE ", Clauses/binary>>
        end,
    Sql1 =
        <<"SELECT * FROM ", TableName/binary,
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
                             Model = ai_db_model:new(ModelName),
                             lists:foldl(FoldFun, Model, Pairs)
                     end,
            Models = lists:map(RowFun, Rows),
            {{ok, Models}, NewState};
        Error -> Error
  end.



find_all(ModelName,State) ->  find_all(ModelName, [], 0, 0, State).
find_all(ModelName,Sort,Limit,Offset,State) ->   find_by(ModelName, [], Sort, Limit, Offset, State).
count(ModelName,State) ->
    TableName = ai_postgres_utils:escape_field(ModelName),
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
    {Values, CleanConditions} = ai_postgres_clause:prepare_conditions(Conditions),
    Clauses = ai_postgres_clause:where_clause(CleanConditions),
    TableName = ai_postgres_utils:escape_field(ModelName),
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
        {ok,_Col,_Row} = epgsql:squery(Conn,"SELECT 1 "),
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

