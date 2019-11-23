-module(ai_postgres_utils).
-export([slot_numbered/1,escape_field/1,escape_value/1]).
-export([escape_operator/1]).

-export([rows_to_proplists/2,rows_to_map/2]).
-include_lib("epgsql/include/epgsql.hrl").

rows_to_proplists(Columns,Rows)->
    ColFun = fun(Col) -> Col#column.name end,
    ColumnNames = lists:map(ColFun, Columns),
    RowFun =
        fun(Row) ->
                Fields = erlang:tuple_to_list(Row),
                lists:zip(ColumnNames, Fields)
        end,
    lists:map(RowFun, Rows).

rows_to_map(Columns,Rows)->
    ColFun = fun(Col) -> Col#column.name end,
    ColumnNames = lists:map(ColFun, Columns),
    RowFun =
        fun(Row) ->
                Fields = erlang:tuple_to_list(Row),
                Pairs = lists:zip(ColumnNames, Fields),
                maps:from_lists(Pairs)

        end,
    lists:map(RowFun, Rows).


slot_numbered({Prefix, N}) ->
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

escape_operator('=<') -> <<"<=">>;
escape_operator('/=') -> <<"!=">>;
escape_operator('==') -> <<"=">>;
escape_operator(Op) -> ai_string:to_string(Op).