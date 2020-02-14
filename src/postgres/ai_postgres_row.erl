-module(ai_postgres_row).
-include_lib("epgsql/include/epgsql.hrl").

-export([to_proplists/2,to_proplists/3]).
-export([to_map/2,to_map/3]).

-spec to_proplists(Columns::list(),Rows::list()) -> list().
to_proplists(Columns,Rows)-> to_proplists(Columns,Rows,fun columns/1).
-spec to_map(Columns::list(),Rows::list()) -> map().
to_map(Columns,Rows)-> to_map(Columns,Rows,fun columns/1).

-spec to_proplists(Columns::list(),Rows::list(),ColFun::function()) -> list().
to_proplists(Columns,Rows,ColFun)->
  ColumnNames = lists:map(ColFun, Columns),
  lists:map(
    fun(Row) ->
        Fields = erlang:tuple_to_list(Row),
        lists:zip(ColumnNames, Fields)
  end,Rows).
-spec to_map(Columns::list(),Rows::list(),ColFun::function()) -> map().
to_map(Columns,Rows,ColFun)->
  ColumnNames = lists:map(ColFun, Columns),
  lists:map(
    fun(Row) ->
        Fields = erlang:tuple_to_list(Row),
        Pairs = lists:zip(ColumnNames, Fields),
        maps:from_list(Pairs)
    end,Rows).

columns(Col)-> erlang:binary_to_atom(Col#column.name, utf8). 
