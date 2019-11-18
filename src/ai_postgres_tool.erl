-module(ai_postgres_tool).
-export([rows_to_proplists/2,rows_to_map/2]).
-export([rows_to_proplists/3,rows_to_map/3]).
-export([fields_to_select/1,fields_to_select/2]).
-export([proplists_to_insert/1,proplists_to_insert/2,proplists_to_insert/3]).
-export([proplists_to_update/1,proplists_to_update/2,proplists_to_update/3]).

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

fields_to_select(Fields)-> fields_to_select(Fields,undefined).
fields_to_select(Fields,undefined)->[escape_field(F) || F <- Fields ];
fields_to_select(Fields,MappedCols)->
    lists:foldl(fun(F,Acc)->
                        case proplists:get_value(F,MappedCols) of
                            undefined -> [escape_field(F)|Acc];
                            Mapped ->
                                Field = escape_field(ai_string:to_string(F)),
                                AsField = escape_field(ai_string:to_string(Mapped)),
                                [<<$\s,Field/binary," AS ",AsField/binary,$\s>>| Acc]
                        end
                end,[],Fields).


proplists_to_insert(Fields)-> proplists_to_insert(Fields,undefined).
proplists_to_insert(Fields,MappedCols,undefined)-> proplists_to_insert(Fields,MappedCols);
proplists_to_insert(Fields,MappedCols,WhiteList)->
    FilterdField = white_list(Fields,WhiteList),
    proplists_to_insert(FilterdField,MappedCols).

proplists_to_insert(Fields,undefined) ->
    Fun = fun({FKey,FValue},{Index,AccKeys,AccPlaces,AccValues})->
                  IndexBin = ai_string:to_string(Index),
                  {Index + 1, [escape_field(FKey)|AccKeys], [<<"$",IndexBin/binary>>|AccPlaces],[FValue|AccValues]}
          end,
    {Index,Keys,Places,Values} =
        lists:foldl(Fun,{1,[],[],[]},Fields),
    {Index,lists:reverse(Keys),lists:reverse(Places),lists:reverse(Values)};

proplists_to_insert(Fields,MappedCols) ->
    RMappedCols = reverse_mapped_cols(MappedCols),
    Fun = fun({FKey,FValue},{Index,AccKeys,AccPlaces,AccValues})->
                  Key =
                      case proplists:get_value(FKey,RMappedCols) of
                          undefined -> FKey;
                          Mapped -> Mapped
                      end,
                  IndexBin = ai_string:to_string(Index),
                  {Index + 1, [escape_field(Key)|AccKeys], [<<"$",IndexBin/binary>>|AccPlaces],[FValue|AccValues]}
          end,
    {Index,Keys,Places,Values} =
        lists:foldl(Fun,{1,[],[],[]},Fields),
    {Index,lists:reverse(Keys),lists:reverse(Places),lists:reverse(Values)}.

proplists_to_update(Fields)-> proplists_to_update(Fields,undefined).
proplists_to_update(Fields,MappedCols,undefined)-> proplists_to_update(Fields,MappedCols);
proplists_to_update(Fields,MappedCols,WhiteList) ->
    FilterdField = white_list(Fields,WhiteList),
    proplists_to_update(FilterdField,MappedCols).

proplists_to_update(Fields,undefined)->
    Fun = fun({FKey,FValue},{Index,Instr,AccValues})->
                  IndexBin = ai_string:to_string(Index),
                  Place = <<"$",IndexBin/binary>>,
                  Key = escape_field(FKey),
                  I = <<"SET "/utf8,Key/binary," = ",Place/binary>>,
                  {Index + 1, [I|Instr],[FValue|AccValues]}
          end,
    {Index,Keys,Values} =
        lists:foldl(Fun,{1,[],[]},Fields),
    {Index,lists:reverse(Keys),lists:reverse(Values)};

proplists_to_update(Fields,MappedCols)->
    RMappedCols = reverse_mapped_cols(MappedCols),
    Fun = fun({FKey,FValue},{Index,Instr,AccValues})->
                  IndexBin = ai_string:to_string(Index),
                  Key =
                      case proplists:get_value(FKey,RMappedCols) of
                          undefined -> escape_field(FKey);
                          Mapped -> escape_field(Mapped)
                      end,
                  Place = <<"$",IndexBin/binary>>,
                  I = <<"SET "/utf8,Key/binary," = ",Place/binary>>,
                  {Index + 1, [I|Instr],[FValue|AccValues]}
          end,
    {Index,Keys,Values} =
        lists:foldl(Fun,{1,[],[]},Fields),
    {Index,lists:reverse(Keys),lists:reverse(Values)}.

escape_field(Field) ->
    F = ai_string:to_string(Field),
    if
        F == <<"*">> -> <<$\s,F/binary,$\s>>;
        true->
            F1 = re:replace(F,"\"","\\\"",[global,{return,binary}]),
            <<"\"",F1/binary,"\"">>
    end.
%% https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
%escape_value(Value)->
%    F = ai_string:to_string(Value),
%    case binary:match(F,<<"'">>) of
%        nomatch ->  <<" '",F,"' ">>;
%        _->
%            F1 = re:replace(F,"'","\\'",[global,{return,binary}]),
%            <<" E'",F1,"' ">>
%    end.
white_list(Fields,WhiteList)->
    lists:foldl(fun(WhiteField,Acc)->
                        case proplists:get_value(WhiteField,Fields) of
                            undefined -> Acc;
                            Value -> [{WhiteField,Value}|Acc]
                        end
                end,[],WhiteList).

reverse_mapped_cols(MappedCols)->
    lists:foldl(fun({Key,MappedKey},Acc)->
                        [{MappedKey,Key}|Acc]
                end,[],MappedCols).
