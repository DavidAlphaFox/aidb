-module(ai_db_orm).
-export([parse_transform/2]).

parse_transform(AST,Options)->
  AST0 = ai_pt_helper:transform(fun trans/1,AST,Options),
  io:format("~p~n",[AST0]),
  AST0.

trans(Ctx)->
  Fields  = ai_pt_helper:directives(Ctx,field),
  SQLModule = ai_pt_helper:directives(Ctx,sql_module),
  ModelName = ai_pt_helper:module_name(Ctx),
  TableName =
    case ai_pt_helper:directives(Ctx,table) of
      [Table] -> Table;
      _ -> ModelName
    end,
  Ctx1 = build_record(Ctx,ModelName,Fields),
  Ctx2 = build_tablename_function(Ctx1,TableName),
  Ctx3 = build_columns_functions(Ctx2,Fields),
  Ctx4 = build_type_functions(Ctx3, Fields),
  Ctx5 = build_common_functions(Ctx4,Fields),
  Ctx6 = build_get_functions(Ctx5, ModelName, Fields),
  Ctx7 = build_set_functions(Ctx6, ModelName, Fields),
  build_new_functions(Ctx7, ModelName).


build_record(Ctx, ModelName,Fields) ->
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
  ai_pt_helper:add_record(Ctx, ModelName, FieldsDef).

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
                         ai_pt:build_atom(Value)) | Clauses];
                    _ -> [ai_pt:build_clause(
                            ai_pt:build_atom(FieldName),
                            ai_pt:build_atom(undefined)) | Clauses]
                  end
              end, [], Fields),
  ai_pt_helper:add_function(Ctx, export, '-type', Clauses).

build_columns_functions(Ctx,Fields)->
  Columns = lists:map(fun({Col,_}) -> Col end,Fields),
  AliasColumns = lists:map(fun(Col)-> alias_column(Col) end,Columns),
  Clauses = lists:foldl(
              fun({as,Col,Alias},Acc)->
                  [ai_pt:build_clause([ai_pt:build_atom(Col)],ai_pt:build_value(Alias))|Acc];
                 (Col,Acc) -> [ai_pt:build_clause([ai_pt:build_atom(Col)],ai_pt:build_value(Col))|Acc]
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


build_common_functions(Ctx, Fields) ->
  Ctx1 = lists:foldl(
           fun({Option, Default}, Acc) ->build_common_function(Acc, Fields, Option, Default) end,
           Ctx, [{autoincrement, {ok, false}},
                 {not_null, {ok, false}},
                 {unique, {ok, false}},
                 {default, {none, null}},
                 {belongs_to, {none, null}}]),
  Primary = lists:filter(
              fun({_,Options})->
                  case lists:keyfind(primary, 1, Options) of
                    {primary,_} ->true;
                    _ ->
                      case lists:keyfind(autoincrement,1,Options) of
                        {autoincrement,_} -> true;
                        _ -> false
                      end
                  end
              end, Fields),
  case Primary of
    [{FieldName,_}] ->
      ai_pt_helper:add_function(Ctx1,export,'-primary_key',
                                ai_pt:build_clause([],ai_pt:build_tuple({ok,FieldName})));
    _ ->
      ai_pt_helper:add_function(Ctx1,export,'-primary',
                                ai_pt:build_clause([],ai_pt:build_tuple({none,undefined})))
  end.

build_common_function(Ctx, Fields, Option, Default) ->
  Clauses = build_common_clauses(Fields, Option, Default),
  ai_pt_helper:add_function(Ctx, export, list_to_atom("-" ++ atom_to_list(Option)), Clauses).

build_common_clauses(Fields, Option, Default) ->
  lists:foldl(
    fun({FieldName, Options}, Clauses) ->
        Value = case lists:keyfind(Option, 1, Options) of
                  {Option, A} -> {ok, A};
                  _ -> Default
                end,
        Clauses ++ [ai_pt:build_clause(ai_pt:build_atom(FieldName),
                                       ai_pt:build_tuple(Value))]
    end, [], Fields).


build_get_functions(Ctx, ModelName, Fields) ->
  lists:foldl(fun({FieldName, Options}, Acc) ->
        case lists:keyfind(type, 1, Options) of
          {type, _} ->
            Clause = ai_pt:build_clause(
                       ai_pt:build_var('R'),
                       ai_pt:build_get_record_field('R', ModelName, FieldName)),
            ai_pt_helper:add_function(Acc, export, FieldName, Clause);
          _ -> Acc
        end
    end, Ctx, Fields).
build_set_functions(Ctx, ModelName, Fields) ->
  lists:foldl(fun({FieldName, Options},Acc) ->
        case lists:keyfind(type, 1, Options) of
          {type, Def} ->
            V = ai_pt:build_var('V'),
            Type = ai_pt:build_atom(Def),
            CastCall = ai_pt:build_call(ai_db_type, cast, [Type, V]),
            V1 = ai_pt:build_var('V1'),
            VT = ai_pt:build_tuple({ok,V1}),
            CastV1 = ai_pt:build_match(VT, CastCall),
            Field = ai_pt:build_record_field(FieldName, V1),
            Record = ai_pt:build_record('R', ModelName, [Field]),
            R = ai_pt:build_var('R'),
            Clause = [
                ai_pt:build_clause([ai_pt:build_atom(undefined), R],[R]),
                ai_pt:build_clause([ai_pt:build_atom(null), R],
                                   ai_pt:build_record('R', ModelName,
                                                      [ai_pt:build_record_field(FieldName, ai_pt:build_atom(null))])),
                ai_pt:build_clause([V, R],[CastV1, Record])],
            ai_pt_helper:add_function(Acc, export, FieldName, Clause);
          _ -> Acc
        end
    end, Ctx, Fields).

build_new_functions(Ctx,ModelName)->
  M = ai_pt:build_var('M'),
  IsMap = ai_pt:build_call(is_map,M),
  Clause0 = ai_pt:build_clause([M],[IsMap],
                               [ai_pt:build_call(new,ai_pt:build_call(maps,to_list,[M]))]),
  L = ai_pt:build_var('L'),
  IsList = ai_pt:build_call(is_list,L),
  R = ai_pt:build_var('R'),
  F = ai_pt:build_var('F'),
  V = ai_pt:build_var('V'),
  Acc = ai_pt:build_var('Acc'),
  FunBody = ai_pt:build_call(ModelName, F, [V, Acc]),
  FunClause = ai_pt:build_clause([ai_pt:build_tuple({F, V}), Acc],FunBody),
  Fun = ai_pt:build_fun(FunClause),
  Body = [ai_pt:build_match(R, ai_pt:build_record(ModelName,[])),
          ai_pt:build_call(lists, foldl, [Fun, R, L])],
  Clause1 = ai_pt:build_clause([L], [IsList],Body),
  ai_pt_helper:add_function(Ctx,export,new,[Clause0,Clause1]).
