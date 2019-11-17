-module(ai_postgres_tool).
-export([rows_to_proplists/2,rows_to_map/2]).

rows_to_proplists(Cols,Rows)->
    Fun = fun(Row,Acc) ->
                  Map = row_to_proplists(Cols,Row),
                  [Map|Acc]
          end,
    case Rows of
        [] -> not_found;
        _ -> lists:foldr(Fun,[],Rows)
    end.

rows_to_map(Cols,Rows)->
    Fun = fun(Row,Acc) ->
                  Map = row_to_proplists(Cols,Row),
                  [maps:from_list(Map)|Acc]
          end,
    case Rows of
        [] -> not_found;
        _ -> lists:foldr(Fun,[],Rows)
    end.


row_to_proplists(Cols,Row)->
    Fun = fun(Col,{Index,Acc})->
                  Key = erlang:element(2,Col),
                  Value  = erlang:element(Index,Row),
                  {Index + 1, [{Key,Value}|Acc]}
          end,
    {_Index,Map} = lists:foldl(Fun,{1,[]},Cols),
    Map.
