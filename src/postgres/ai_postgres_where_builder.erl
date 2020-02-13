-module(ai_postgres_where_builder).
-include("ai_db_query.hrl").
-export([build/1]).

build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{where = undefined},Ctx) -> Ctx;
build(#ai_db_query{where = []},Ctx)-> Ctx;
build(#ai_db_query{where = Where},
      #ai_db_query_context{ sql = Sql,bindings = Bindings,slot = Slot } = Ctx)->
  {Values,Exprs,Count} = transform_where(Where,{[], [], Slot}),
  WhereSql = build_where(Exprs),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," WHERE ",WhereSql/binary>>,
    bindings = Bindings ++ lists:reverse(Values),
    slot = Count
   }.

transform_where(Exprs, Acc) when is_list(Exprs) ->
  lists:foldl(fun transform_where/2, Acc, Exprs);

transform_where({LogicalOp, Exprs},
                {Values, CleanExprs, Count})
  when (LogicalOp == 'and')
       or (LogicalOp == 'or')
       or (LogicalOp == 'not') ->
    {NewValues, NewCleanExprs, NewCount} = transform_where(Exprs, {Values, [], Count}),
    {NewValues,
        [{LogicalOp, lists:reverse(NewCleanExprs)} | CleanExprs],
    NewCount};

%% in, not in, any, some and all on Array
%% currently we only support in/not in
transform_where({Op,Name, InValues},
                {Values, CleanExprs, Count})
  when erlang:is_list(InValues)
       and ((Op == 'in') or (Op == 'not_in')) ->
  InValues0 = lists:reverse(InValues),
  Length = erlang:length(InValues),
  Holders = lists:seq(Count,Count + Length -1),
  {
   InValues0 ++ Values,
   [{Op,Name,{'$',Holders}}| CleanExprs],
   Count + Length
  };
%% in, not in, exits, not exits, any, some on and all subquery
%% currently we only support in/not in, exists/not exists
transform_where({_Op,_Name,{subquery,_}} = Cond,
               {Values,CleanExprs,Count})->
  { Values,[Cond| CleanExprs],Count };

transform_where({_Op,_Name,{sql,_}} = Cond,
               {Values,CleanExprs,Count})->
  { Values,[Cond| CleanExprs],Count };
%% 仅当是tuple的时候才是两个field操作
transform_where({_Op,_Name1, {field,_}} = Cond,
                {Values, CleanExprs, Count}) ->
  {Values,[Cond | CleanExprs], Count};
%% 特殊操作符号
transform_where({Op,_Name,_Value} = Cond,
               {Values,CleanExprs,Count})
  when (Op == 'is') or (Op == 'is_not') ->
  {Values,[Cond| CleanExprs],Count};

transform_where({Op,Name, Value},
                {Values, CleanExprs, Count}) ->
    {
     [Value | Values],
     [{Op,Name, {'$', Count}} | CleanExprs],
     Count + 1
    };
%% 直接相等操作
transform_where({Name1, {field,_} = Name2},
                {Values, CleanExprs, Count}) ->
    {
     Values,
     [{'==',Name1, Name2} | CleanExprs],
     Count
    };
transform_where({Name, Value},
                {Values, CleanExprs, Count})
  when Value =/= 'null'
       andalso Value =/= 'not_null'
       andalso (not erlang:is_boolean(Value))->
    {
     [Value | Values],
     [{'==',Name, {'$', Count}} | CleanExprs],
     Count + 1
    };
transform_where({Name, not_null}, {Values, CleanExprs, Count}) ->
    {
     Values,
     [{is_not, Name, null} | CleanExprs],
     Count
    };
transform_where({Name, Value}, {Values, CleanExprs, Count}) ->
    {
     Values,
     [{is,Name, Value} | CleanExprs],
     Count
    };
transform_where([], Acc) -> Acc;
transform_where(Expr, _) -> throw({unsupported,expression, Expr}).


build_where([]) -> <<>>;
build_where(Exprs) when erlang:is_list(Exprs) ->
  Clauses = [build_where(Expr) || Expr <- Exprs],
  ai_string:join(Clauses,<<" AND ">>);
build_where({'and', Exprs}) ->
  BinaryClauses = build_where(Exprs),
  <<" ( ",BinaryClauses/binary," ) ">>;
build_where({'or', Exprs}) ->
  Clauses = [build_where(Expr) || Expr <- Exprs],
  BinaryClauses = ai_string:join(Clauses,<<" OR ">>),
  <<" ( ",BinaryClauses/binary," ) ">>;
build_where({'not', Expr}) ->
  BinaryClauses = build_where(Expr),
  <<" NOT ( ",BinaryClauses/binary," ) ">>;
build_where({is,Name,Value}) ->
  N = ai_postgres_escape:escape_field(Name),
  V = ai_string:to_string(Value),
  <<N/binary," IS ",V/binary>>;
build_where({is_not,Name,Value})->
  N = ai_postgres_escape:escape_field(Name),
  V = ai_string:to_string(Value),
  <<N/binary," IS NOT ",V/binary>>;
build_where({in,Name,{'$',Holders}}) ->
  Slots =
    lists:map(
      fun(H)-> ai_postgres_escape:slot_numbered({'$',H}) end,
      Holders),
  Slots0 = ai_string:join(Slots,<<",">>),
  N = ai_postgres_escape:escape_field(Name),
  <<N/binary," IN ( ",Slots0/binary," )">>;
build_where({Op,Name1,{field,Name2}}) ->
  N1 = ai_postgres_escape:escape_field(Name1),
  N2 = ai_postgres_escape:escape_field(Name2),
  O = ai_postgres_escape:escape_operator(Op),
  <<N1/binary,O/binary,N2/binary>>;
build_where({Op, Name, { '$', _ } = Slot}) ->
  P = ai_postgres_escape:slot_numbered(Slot),
  N = ai_postgres_escape:escape_field(Name),
  O = ai_postgres_escape:escape_operator(Op),
  <<N/binary,O/binary,P/binary>>.
