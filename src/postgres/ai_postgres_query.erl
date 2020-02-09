-module(ai_postgres_query).
-export([select/3,select/4,select/6,select/7]).
-export([count/1,count/2,count/3]).
-export([insert/3,update/3,delete/2]).

-spec delete(Table::atom() | binary(),
             Conditions::list()) -> tuple().
delete(Table, Conditions) ->
    TableName = ai_postgres_escape:escape_field(Table),
    {_Select, Where, Values} = form_select_query([], Conditions,undefined),
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    Sql = <<"DELETE FROM ",TableName/binary,WhereClause/binary>>,
    {Sql,Values}.

-spec update(Table::atom() | binary(), Columns::map() | list(),
            Conditions::list() ) -> tuple().
update(Table,Columns,Conditions) when erlang:is_map(Columns)->
    update(Table,maps:to_list(Columns),Conditions);
update(Table,Columns,Conditions)->
    TableName = ai_postgres_escape:escape_field(Table),
    {_Select, Where, Values} = form_select_query([], Conditions, undefined),
    {_,UFields, UValues} =
        lists:foldr(
          fun({K, V}, {Index,Fs, Vs}) ->
                  IndexBin = erlang:integer_to_binary(Index),
                  FieldName = ai_postgres_escape:escape_field(K),
                  Slot = <<FieldName/binary, " = $", IndexBin/binary>>,
                  {Index+1,[Slot|Fs],[V|Vs]}
          end, {erlang:length(Values) + 1,[], []}, Columns),
    UpdateSlotsCSV = ai_string:join(UFields, <<",">>),
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    UpdateSql = <<"UPDATE ", TableName/binary," SET ",UpdateSlotsCSV/binary,WhereClause/binary>>,
    {UpdateSql,Values ++ lists:reverse(UValues)}.

-spec insert(Table::atom() | binary(), IDColumnName::atom() | binary(),
             Columns::map() | list()) -> tuple().
insert(Table,IDColumnName,Columns) when erlang:is_map(Columns)->
    insert(Table,IDColumnName,maps:to_list(Columns));
insert(Table,IDColumnName,Columns) ->
    IDColumnName0 = ai_postgres_escape:escape_field(IDColumnName),
    TableName = ai_postgres_escape:escape_field(Table),
    {Fields, Values} =
        lists:foldr(
          fun({K, V}, {Fs, Vs}) ->
                  {[ai_postgres_escape:escape_field(K)|Fs], [V|Vs]}
          end, {[], []}, Columns),

    SlotsFun = fun(N) -> NBin = erlang:integer_to_binary(N), <<"$",NBin/binary>>  end,
    InsertSlots = lists:map(SlotsFun, lists:seq(1, erlang:length(Values))),
    FieldsCSV = ai_string:join(Fields, <<" , ">>),
    InsertSlotsCSV = ai_string:join(InsertSlots, <<" , ">>),
    InsertSql = <<"INSERT INTO ", TableName/binary," ( ", FieldsCSV/binary, " ) ",
                              " VALUES "," ( ", InsertSlotsCSV/binary, " ) ",
                              "RETURNING ", IDColumnName0/binary>>,
    {InsertSql,Values}.

-spec count(Table::atom() | binary()) -> tuple().
count(Table) -> count(Table,[],undefined).
-spec count(Table::atom() | binary(),
           Conditions::list()) -> tuple().
count(Table,Conditions) -> count(Table,Conditions,undefined).
-spec count(Table::atom() | binary(),Conditions::list(),
           ExtraWhere::binary()|undefined) -> tuple().
count(Table,Conditions,ExtraWhere) ->
    {_Select, Where, Values} =
        form_select_query([], Conditions, ExtraWhere),
    TableName = ai_postgres_escape:escape_field(Table),
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    TableName = ai_postgres_escape:escape_field(Table),
    Sql = <<"SELECT COUNT(*) AS total FROM ", TableName/binary,WhereClause/binary>>,
    {Sql,Values}.

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list()) -> tuple().
select(Table,SelectColumns,Conditions)->
    select(Table,SelectColumns,Conditions,undefined,undefined,0,0).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),ExtraWhere::binary() | undefiend) -> tuple().
select(Table,SelectColumns,Conditions,ExtraWhere)->
    select(Table,SelectColumns,Conditions,ExtraWhere,undefined,0,0).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),OrderBy::list(),
             Limit::integer(),Offset::integer()) -> tuple().
select(Table,SelectColumns,Conditions,OrderBy,Limit,Offset)->
    select(Table,SelectColumns,Conditions,undefined,OrderBy,Limit,Offset).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),ExtraWhere::binary() | undefiend,
             OrderBy::list(),Limit::integer(),Offset::integer()) -> tuple().
select(Table, SelectColumns,Conditions, ExtraWhere,
       OrderBy, Limit, Offset) ->
    {Select, Where, Values} =
        form_select_query(SelectColumns, Conditions, ExtraWhere),
    OrderByClause =
        case OrderBy of
            undefined -> <<>>;
            [] -> <<>>;
            OrderBy when erlang:is_binary(OrderBy)-> <<" ORDER BY ",OrderBy/binary>>;
            OrderBy when erlang:is_list(OrderBy)  ->
                OrderByBin = ai_postgres_clause:order_by(OrderBy),
                case erlang:byte_size(OrderByBin) of
                    0 -> <<>>;
                    _ -> <<" ORDER BY ",OrderByBin/binary>>
                end
        end,
    WhereClause =
        case erlang:byte_size(Where) of
            0 -> <<>>;
            _ -> <<" WHERE ", Where/binary>>
        end,
    TableName = ai_postgres_escape:escape_field(Table),
    Sql1 = <<"SELECT ",Select/binary, " FROM ", TableName/binary,
             WhereClause/binary," ",OrderByClause/binary>>,
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
  
form_select_query(SelectColumns, Conditions, ExtraWhere) ->
    {Values, CleanConditions} = ai_postgres_clause:prepare_conditions(Conditions),
    WhereTmp = ai_postgres_clause:where(CleanConditions),
    SFields = [ai_postgres_escape:escape_field(F) || F <- SelectColumns],
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
