-module(ai_db_model).
-export([
         model_name/1,
         model_module/1,
         model_fields/1,
         new_model/1,
         new_model/2,
         get_field/2,
         set_field/3
        ]).
-export([wakeup/1,sleep/2]).
-export([build_fields/2,build_fields/3]).
-compile({inline,[build_fields/4]}).


build_fields(Schema,Input)->
  Fields = ai_db_schema:schema_fields(Schema),
  lists:foldl(
    fun(#{name := Key},Acc)->
        KeyBin = ai_string:to_string(Key),
        build_fields(Key,KeyBin,Input,Acc)
  end,#{},Fields).

build_fields(Schema,Emnu,Input)->
  Fields = ai_db_schema:schema_fields(Schema),
  lists:foldl(
    fun(#{name := Key},Acc)->
        case lists:member(Key,Emnu) of
          true ->
            KeyBin = ai_string:to_string(Key),
            build_fields(Key,KeyBin,Input,Acc);
          _ -> Acc
        end
    end,#{},Fields).

build_fields(Key,KeyBin,Input,Acc)->
  case maps:get(KeyBin,Input,undefined) of
    undefined -> Acc;
    Value -> Acc#{Key => Value}
  end.



model_name(Model) -> maps:get(name, Model, undefined).
model_module(Model) -> maps:get(module, Model, undefined).
model_fields(Model) -> maps:get(fields, Model, #{}).

get_field(Name,Model) -> maps:get(Name, model_fields(Model), undefined).

set_field(FieldName, FieldValue, Model = #{fields := Fields}) ->
  maps:put(fields, maps:put(FieldName, FieldValue, Fields), Model);
set_field(FieldName, FieldValue, Fields) ->
  maps:put(FieldName, FieldValue, Fields).

new_model(Name) -> new_model(Name, #{}).

new_model(Name, Fields) ->
  Module = ai_db_manager:attr_value(Name, module),
  #{name => Name, module => Module, fields => Fields}.

wakeup(Model) ->
  Module = model_module(Model),
  Fields = model_fields(Model),
  Module:wakeup(Fields).

sleep(Name,Model)->
  Module = ai_db_manager:attr_value(Name,module),
  Fields = Module:sleep(Model),
  #{name => Name, module => Module, fields => Fields}.
