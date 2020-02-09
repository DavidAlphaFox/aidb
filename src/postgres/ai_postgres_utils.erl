-module(ai_postgres_utils).
-compile({inline,[
                  read_after_insert/3,
                  read_after_update/3
                 ]}).
-export([
         rows_to_proplists/2,
         rows_to_map/2,
         rows_to_proplists/3,
         rows_to_map/3
        ]).
-export([
         read_after_insert/3,
         read_after_update/3
        ]).
-include_lib("epgsql/include/epgsql.hrl").


rows_to_proplists(Columns,Rows)->
    rows_to_proplists(Columns,Rows,atom).
rows_to_map(Columns,Rows)->
    rows_to_map(Columns,Rows,atom).

rows_to_proplists(Columns,Rows,ColumnType)->
    ColFun = column(ColumnType),
    ColumnNames = lists:map(ColFun, Columns),
    RowFun =
        fun(Row) ->
                Fields = erlang:tuple_to_list(Row),
                lists:zip(ColumnNames, Fields)
        end,
    lists:map(RowFun, Rows).

rows_to_map(Columns,Rows,ColumnType)->
    ColFun = ColFun = column(ColumnType),
    ColumnNames = lists:map(ColFun, Columns),
    RowFun =
        fun(Row) ->
                Fields = erlang:tuple_to_list(Row),
                Pairs = lists:zip(ColumnNames, Fields),
                maps:from_list(Pairs)
        end,
    lists:map(RowFun, Rows).


read_after_insert(Conn,InsertFun,ReadFun)->
  {InsertQuery,InsertValues} = InsertFun(),
  case epgsql:equery(Conn, InsertQuery, InsertValues) of
    {ok,Count,Cols,Rows} ->
      if Count == 1 ->
          {ReadQuery,ReadValues} = ReadFun(Cols,Rows),
          epgsql:equery(Conn,ReadQuery,ReadValues);
         true -> error(not_persist)
      end;
    Error -> error(Error)
  end.

read_after_update(Conn,UpdateFun,ReadFun)->
  {UpdateQuery,UpdateValues} = UpdateFun(),
  case epgsql:equery(Conn, UpdateQuery, UpdateValues) of
    {ok,Count} ->
      if Count == 1 ->
          {ReadQuery,ReadValues} = ReadFun(),
          epgsql:equery(Conn,ReadQuery,ReadValues);
         true -> error(not_persist)
      end;
    Error -> error(Error)
  end.
%%%===================================================================
%%% Internal functions
%%%===================================================================

column(atom)->
    fun(Col) -> erlang:binary_to_atom(Col#column.name, utf8) end;
column(_) ->
    fun(Col) -> Col#column.name end.
