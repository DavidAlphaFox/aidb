-module(ai_postgres_query).
-export([select/3,select/4,select/6,select/7]).
-export([count/1,count/2,count/3]).

count(Table) -> count(Table,[],undefined).
count(Table,Conditions) -> count(Table,Conditions,undefined).
count(Table, Conditions, ExtraWhere) ->
  {_Select, Where, Values} =
    form_select_query([], Conditions, ExtraWhere),
    TableName = ai_postgres_utils:escape_field(Table),
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    TableName = ai_postgres_utils:escape_field(Table),
    Sql = <<"SELECT COUNT(*) AS total FROM ", TableName/binary,WhereClause/binary>>,
    {Sql,Values}.

select(Table,SelectFields,Conditions)->
    select(Table,SelectFields,Conditions,undefined,undefined,0,0).
select(Table,SelectFields,Conditions,ExtraWhere)->
    select(Table,SelectFields,Conditions,ExtraWhere,undefined,0,0).
select(Table,SelectFields,Conditions,OrderBy,Limit,Offset)->
    select(Table,SelectFields,Conditions,undefined,OrderBy,Limit,Offset).
select(Table, SelectFields,Conditions, ExtraWhere,
       OrderBy, Limit, Offset) ->
    {Select, Where, Values} =
        form_select_query(SelectFields, Conditions, ExtraWhere),
    OrderByClause =
        case OrderBy of
            undefined -> <<>>;
            [] -> <<>>;
            OrderBy when erlang:is_binary(OrderBy)->
                <<"ORDER BY ",OrderBy/binary>>;
            OrderBy when erlang:is_list(OrderBy)  ->
                OrderByBin = ai_postgres_clause:order_by_clause(OrderBy),
                <<"ORDER BY ",OrderByBin/binary>>
        end,
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    TableName = ai_postgres_utils:escape_field(Table),
    Sql1 = <<"SELECT ",Select/binary, " FROM ", TableName/binary,
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
    {Sql2,AllValues}.
  
form_select_query(SelectFields, Conditions, ExtraWhere) ->
    {Values, CleanConditions} = ai_postgres_clause:prepare_conditions(Conditions),
    WhereTmp = ai_postgres_clause:where_clause(CleanConditions),
    SFields = [ai_postres_utils:escape_field(F) || F <- SelectFields],
    Where =
        case ExtraWhere of
            undefined -> WhereTmp;
            _ ->
                case erlang:byte_size(WhereTmp) of
                    0 -> ExtraWhere;
                    _ -> <<WhereTmp/binary," AND ",ExtraWhere/binary>>
                end
        end,
  Select = ai_string:join(SFields, <<",">>),
  {Select, Where, Values}.
