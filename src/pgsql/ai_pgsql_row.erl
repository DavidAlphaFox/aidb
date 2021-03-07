-module(ai_pgsql_row).
-include_lib("epgsql/include/epgsql.hrl").
-export([to_map/2,to_map/3]).

-spec to_map(Columns::list(),Rows::list()) -> map().
to_map(Columns,Rows)->
  to_map(Columns,Rows,fun column/1).
-spec to_map(Columns::list(),Rows::list(),
             ColFun::function()) -> map().
to_map(Columns,Rows,ColFun)->
  ColumnNames = columns(Columns,ColFun),
  lists:map(
    fun(Row) ->
        Fields = erlang:tuple_to_list(Row),
        Pairs = lists:zip(ColumnNames, Fields),
        maps:from_list(Pairs)
    end,Rows).
columns(Columns,atom)->
  lists:map(fun(Col)->column(Col) end,Columns);
columns(Columns,undefined)->
  lists:map(fun(Col)-> Col#column.name end,Columns);
columns(Columns,ColFun) when erlang:is_function(ColFun,1)->
  lists:map(fun(Col)-> ColFun(Col#column.name) end,Columns).
column(ColName)-> erlang:binary_to_atom(ColName, utf8).
