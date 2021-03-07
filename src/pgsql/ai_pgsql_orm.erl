-module(ai_pgsql_orm).
-export([parse_transform/2]).

parse_transform(AST,Options)->
  AST0 = ai_pt_helper:transform(fun trans/1,AST,Options),
  io:format("~p~n",[AST0]),
  AST0.

trans(Ctx)->
  Fields  = ai_pt_helper:directives(Ctx,field),
  TableName = ai_pt_helper:module_name(Ctx),
  TableName1 =
    case ai_pt_helper:directives(Ctx,table) of
      [Table] -> Table;
      _ -> TableName
    end,
  Ctx1 = build_record(Ctx,TableName1,Fields),
  Ctx2 = build_tablename_function(Ctx1,TableName1),
  Ctx3 = build_columns_functions(Ctx2,Fields),
  build_type_functions(Ctx3, Fields).

build_record(Ctx, TableName, Fields) ->
  FieldsDef = lists:foldl(
                fun({Name, Options}, Acc) ->
                    Acc ++ case lists:keyfind(type, 1, Options) of
                             {type, Type} -> [{Name, Type}];
                             _ ->
                               case lists:keyfind(habtm, 1, Options) of
                                 {habtm, Ref} -> [{list_to_atom(atom_to_list(Ref) ++ "_habtm"), term}];
                                 _ -> []
                               end
                           end
                end, [], Fields),
  ai_pt_helper:add_record(Ctx, TableName, FieldsDef).

build_tablename_function(Ctx,TableName) ->
  ai_pt_helper:add_function(Ctx, export, '-table',
                            ai_pt:build_clause([], ai_pt:build_value(TableName))).

build_type_functions(Ctx, Fields) ->
  Clauses = lists:foldl(
              fun({FieldName, Options}, Clauses) ->
                  case lists:keyfind(type, 1, Options) of
                    {type, Value} ->
                      [ai_pt:build_clause(
                         ai_pt:build_atom(FieldName),
                         ai_pt:build_value(Value)) | Clauses];
                    _ -> Clauses
                  end
              end, [], Fields),
  ai_pt_helper:add_function(Ctx, export, '-type', Clauses).

build_columns_functions(Ctx,Fields)->
  Columns = lists:map(fun({Col,_}) -> Col end,Fields),
  AliasColumns = lists:map(fun(Col)-> alias_column(Col) end,Columns),
  Clauses = lists:foldl(
              fun({as,Col,Alias},Acc)->
                  [ai_pt:build_clause([ai_pt:build_value(Col)],ai_pt:build_value(Alias))|Acc];
                 (Col,Acc) -> [ai_pt:build_clause([ai_pt:build_value(Col)],ai_pt:build_value(Col))|Acc]
              end,[],AliasColumns),
  Ctx0 = ai_pt_helper:add_function(Ctx,export,'-alias',Clauses),
  Ctx1 = ai_pt_helper:add_function(Ctx0, export, '-columns',
                                   ai_pt:build_clause([], ai_pt:build_value(Columns))),
  ai_pt_helper:add_function(Ctx1,export,'-alias_columns',
                            ai_pt:build_clause([], ai_pt:build_value(AliasColumns))).
%% 列别名
alias_column(Col)->
  Col1 = erlang:atom_to_list(Col),
  L = string:split(Col1,"_"),
  Length = erlang:length(L),
  if Length > 1 ->
      [H|T] = L,
      T1 = lists:map(
             fun(I) ->
                 [CH|CR] = I,
                 CH1 = string:uppercase([CH]),
                 [CH1|CR]
             end,T),
      {as,Col,erlang:list_to_atom(lists:flatten([H|T1]))};
     true -> Col
  end.