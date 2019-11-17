-module(ai_postgres_tool).
-export([rows_to_proplists/2,rows_to_map/2]).
-export([rows_to_proplists/3,rows_to_map/3]).

rows_to_proplists(Cols,Rows)-> rows_to_proplists(Cols,Rows,undefined).
rows_to_map(Cols,Rows)-> rows_to_map(Cols,Rows,undefined).

rows_to_proplists(Cols,Rows,MappedCols)->
    Fun = fun(Row,Acc) ->
                  Map = row_to_proplists(Cols,Row,MappedCols),
                  [Map|Acc]
          end,
    case Rows of
        [] -> not_found;
        _ -> lists:foldr(Fun,[],Rows)
    end.

rows_to_map(Cols,Rows,MappedCols)->
    Fun = fun(Row,Acc) ->
                  Map = row_to_proplists(Cols,Row,MappedCols),
                  [maps:from_list(Map)|Acc]
          end,
    case Rows of
        [] -> not_found;
        _ -> lists:foldr(Fun,[],Rows)
    end.

row_to_proplists(Cols,Row,undefined)->
    Fun = fun(Col,{Index,Acc})->
                  Key = erlang:element(2,Col),
                  Value  = erlang:element(Index,Row),
                  {Index + 1, [{Key,Value}|Acc]}
          end,
    {_Index,Map} = lists:foldl(Fun,{1,[]},Cols),
    lists:reverse(Map);
row_to_proplists(Cols,Row,MappedCols) when erlang:is_map(MappedCols) ->
    row_to_proplists(Cols,Row,maps:to_list(MappedCols));
row_to_proplists(Cols,Row,MappedCols)->
    Fun = fun(Col,{Index,Acc}) ->
                  Key = erlang:element(2,Col),
                  MappedKey =
                      case proplists:get_value(Key,MappedCols) of
                          undefined -> Key;
                          Mapped -> Mapped
                      end,
                  Value = erlang:element(Index,Row),
                  {Index + 1, [{MappedKey,Value}|Acc]}
          end,
    {_Index,Map} = lists:foldl(Fun,{1,[]},Cols),
    lists:reverse(Map).
