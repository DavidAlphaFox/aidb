-module(ai_db_transform).

-export([
         fields/2,
         conditions/4
        ]).

-spec model(function(),map())-> [map()].
fields(Fun, Model) ->
  ModelName = ai_db_model:name(Model),
  Schema = ai_db_schema:schema(ModelName),
  SchemaFields = ai_db_schema:fields(Schema),
  ModelFields =  ai_db_model:fields(Model),
  lists:foldl(fun(Field, Acc) ->
                  FieldType = ai_db_field:type(Field),
                  FieldName = ai_db_field:name(Field),
                  FieldValue =  ai_db_model:get_field(FieldName, Model),
                  FieldAttrs = ai_db_field:attrs(Field),
                  NewValue = Fun(FieldType, FieldName, FieldValue, FieldAttrs),
                  maps:put(FieldName, NewValue, Acc)
              end, ModelFields, SchemaFields).

-spec conditions(function(),atom(),[tuple()]| tuple(),atom()) -> list().
conditions(Fun, ModelName, Conditions, FieldTypes) when is_list(Conditions) ->
  DTFields = filter_fields_by(ModelName, FieldTypes),
  lists:foldl(fun
                  ({K, V}, Acc) when K == 'and'; K == 'or' ->
                      [{K, conditions(Fun, ModelName, V, FieldTypes)} | Acc];
                  ({'not', V}, Acc) ->
                      [NotCond] = conditions(Fun, ModelName, [V], FieldTypes),
                      [{'not', NotCond} | Acc];
                  ({K, V} = KV, Acc) ->
                      case lists:keyfind(K, 1, DTFields) of
                          {K, FieldType, _} -> [{K, Fun({FieldType, K, V})} | Acc];
                          false -> [KV | Acc]
                      end;
                  ({K, Op, V} = KV, Acc) ->
                      case lists:keyfind(K, 1, DTFields) of
                          {K, FieldType, _} -> [{K, Op, Fun({FieldType, K, V})} | Acc];
                          false -> [KV | Acc]
                      end;
                  (Cond, Acc) -> [Cond | Acc]
              end, [], Conditions);
conditions(Fun, ModelName, Conditions, FieldTypes) -> conditions(Fun, ModelName, [Conditions], FieldTypes).

%%%===================================================================
%%% Internal functions
%%%===================================================================

filter_fields_by(ModelNameOrModel, FilteredFieldTypes)
  when is_atom(ModelNameOrModel) ->
  filter_fields(ModelNameOrModel, undefined, FilteredFieldTypes);
filter_fields_by(ModelNameOrModel, FilteredFieldTypes) ->
  ModelName = ai_db_model:name(ModelNameOrModel),
  filter_fields(ModelName, ModelNameOrModel, FilteredFieldTypes).


filter_fields(ModelName, Model, FilteredFieldTypes) ->
  Schema = ai_db_schema:schema(ModelName),
  SchemaFields = ai_db_schema:fields(Schema),
  lists:foldl(fun(Field, Acc) ->
                  FieldType = ai_db_field:type(Field),
                  case lists:member(FieldType, FilteredFieldTypes) of
                    true -> [set_filter(Field, Model) | Acc];
                    _    -> Acc
                  end
              end, [], SchemaFields).

set_filter(Field, Model) ->
  FieldType = ai_db_field:type(Field),
  FieldName = ai_db_field:name(Field),
  FieldValue =
    case Model /= undefined of
      true -> ai_db_model:get_field(FieldName, Model);
      _    -> undefined
    end,
  {FieldName, FieldType, FieldValue}.
