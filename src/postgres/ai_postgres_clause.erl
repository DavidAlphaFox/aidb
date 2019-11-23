-module(ai_postgres_clause).

-export([prepare_conditions/1,where_clause/1]).
-export([order_by_clause/1]).

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

where_clause([]) -> <<>>;
where_clause(Exprs) when erlang:is_list(Exprs) ->
    Clauses = [where_clause(Expr) || Expr <- Exprs],
    ai_string:join(Clauses,<<" AND ">>);
where_clause({'and', Exprs}) ->
    BinaryClauses = where_clause(Exprs),
    <<" ( ",BinaryClauses/binary," ) ">>;
where_clause({'or', Exprs}) ->
    Clauses = [where_clause(Expr) || Expr <- Exprs],
    BinaryClauses = ai_string:join(Clauses,<<" OR ">>),
    <<" ( ",BinaryClauses/binary," ) ">>;
where_clause({'not', Expr}) ->
    BinaryClauses = where_clause(Expr),
    <<" NOT ( ",BinaryClauses/binary," ) ">>;
where_clause({Op, Name, { '$', _ } = Slot}) ->
    P = ai_postgres_utils:slot_numbered(Slot),
    N = ai_postgres_utils:escape_field(Name),
    O = ai_postgres_utils:escape_operator(Op),
    <<N/binary,O/binary,P/binary>>;
where_clause({Op,Name1, {field,Name2}}) ->
    N1 = ai_postgres_utils:escape_field(Name1),
    N2 = ai_postgres_utils:escape_field(Name2),
    O = ai_postgres_utils:escape_operator(Op),
    <<N1/binary,O/binary,N2>>;
where_clause({Op,Name1,Name2}) ->
    N1 = ai_postgres_utils:escape_field(Name1),
    N2 = ai_postgres_utils:escape_field(Name2),
    O = ai_postgres_utils:escape_operator(Op),
    <<N1/binary,O/binary,N2/binary>>;
where_clause({Name, null}) ->
    N = ai_postgres_utils:escape_field(Name),
    <<N/binary," IS NULL ">>;
where_clause({Name, not_null})->
    N = ai_postgres_utils:escape_field(Name),
    <<N/binary," IS NOT NULL ">>;
where_clause({Name,{'$',_} = Slot}) ->
    N = ai_postgres_utils:escape_field(Name),
    P = ai_postgres_utils:slot_numbered(Slot),
    <<N/binary," = ",P/binary>>;
where_clause({Name,Value}) ->
    N = ai_postgres_utils:escape_field(Name),
    V = ai_postgres_utils:escape_value(Value),
    <<N/binary," = ",V/binary>>.

order_by_clause(SortFields) ->
    ClauseFun = fun({Name, SortOrder}) ->
                        NameBin = ai_string:to_string(Name),
                        OrderBin = ai_string:to_string(SortOrder),
                        <<NameBin/binary," ",OrderBin/binary>>
                end,
    Clauses = lists:map(ClauseFun, SortFields),
    ai_string:join(Clauses,<<",">>).
