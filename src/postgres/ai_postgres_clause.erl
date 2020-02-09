-module(ai_postgres_clause).

-export([
         prepare_conditions/1,
         where/1,
         order_by/1
        ]).

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
%% 仅当是tuple的时候才是两个field操作
prepare_conditions({Op,Name1, {field,_} = Name2}, {Values, CleanExprs, Count}) ->
    {Values,
     [{Op,Name1,Name2} | CleanExprs],
     Count};
prepare_conditions({Op,Name, Value}, {Values, CleanExprs, Count}) ->
    {
     [Value | Values],
     [{Op,Name, {'$', Count}} | CleanExprs],
     Count + 1
    };
%% 直接相等操作
prepare_conditions({Name1, {field,_} = Name2}, {Values, CleanExprs, Count}) ->
    {
     Values,
     [{Name1, Name2} | CleanExprs],
     Count
    };
prepare_conditions({Name, Value}, {Values, CleanExprs, Count})
  when Value =/= 'null'
       andalso Value =/= 'not_null' ->
    {
     [Value | Values],
     [{Name, {'$', Count}} | CleanExprs],
     Count + 1
    };
prepare_conditions({Name, Value}, {Values, CleanExprs, Count}) ->
    {
     Values,
     [{Name, Value} | CleanExprs],
     Count
    };
prepare_conditions([], Acc) -> Acc;
prepare_conditions(Expr, _) -> throw({unsupported,expression, Expr}).

where([]) -> <<>>;
where(Exprs) when erlang:is_list(Exprs) ->
    Clauses = [where(Expr) || Expr <- Exprs],
    ai_string:join(Clauses,<<" AND ">>);
where({'and', Exprs}) ->
    BinaryClauses = where(Exprs),
    <<" ( ",BinaryClauses/binary," ) ">>;
where({'or', Exprs}) ->
    Clauses = [where(Expr) || Expr <- Exprs],
    BinaryClauses = ai_string:join(Clauses,<<" OR ">>),
    <<" ( ",BinaryClauses/binary," ) ">>;
where({'not', Expr}) ->
    BinaryClauses = where(Expr),
    <<" NOT ( ",BinaryClauses/binary," ) ">>;
where({Op, Name, { '$', _ } = Slot}) ->
    P = ai_postgres_utils:slot_numbered(Slot),
    N = ai_postgres_utils:escape_field(Name),
    O = ai_postgres_utils:escape_operator(Op),
    <<N/binary,O/binary,P/binary>>;
where({Op,Name1, {field,Name2}}) ->
    N1 = ai_postgres_utils:escape_field(Name1),
    N2 = ai_postgres_utils:escape_field(Name2),
    O = ai_postgres_utils:escape_operator(Op),
    <<N1/binary,O/binary,N2/binary>>;
where({Name, null}) ->
    N = ai_postgres_utils:escape_field(Name),
    <<N/binary," IS NULL ">>;
where({Name, not_null})->
    N = ai_postgres_utils:escape_field(Name),
    <<N/binary," IS NOT NULL ">>;
where({Name1, {field,Name2}}) ->
    N1 = ai_postgres_utils:escape_field(Name1),
    N2 = ai_postgres_utils:escape_field(Name2),
    <<N1/binary," = ",N2/binary>>;
where({Name,{'$',_} = Slot}) ->
    N = ai_postgres_utils:escape_field(Name),
    P = ai_postgres_utils:slot_numbered(Slot),
    <<N/binary," = ",P/binary>>;
where({Name,Value}) ->
    N = ai_postgres_utils:escape_field(Name),
    V = ai_postgres_utils:escape_value(Value),
    <<N/binary," = ",V/binary>>.

order_by(SortFields) ->
    ClauseFun =
        fun({Name, SortOrder}) ->
                NameBin = ai_string:to_string(Name),
                OrderBin = ai_string:to_string(SortOrder),
                <<NameBin/binary," ",OrderBin/binary>>
        end,
    Clauses = lists:map(ClauseFun, SortFields),
    ai_string:join(Clauses,<<",">>).
